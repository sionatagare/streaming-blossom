//! Dual Driver Tracked
//!
//! a dual driver that handles the event of blossom hitting zero in software,
//! while passing through all the other functionalities to another driver
//!

use crate::blossom_tracker::*;
use crate::dual_module_stackless::*;
use crate::interface::*;
use crate::util::*;

pub trait DualTrackedDriver {
    /// with `DualDriverTracked`, the driver doesn't need to report any `BlossomNeedExpand` obstacles.
    /// the external driver should not grow more than this value before returning, to accommodate with this offloading.
    fn find_conflict(&mut self, maximum_growth: CompactWeight) -> (CompactObstacle, CompactWeight);
}

pub struct DualDriverTracked<D: DualStacklessDriver + DualTrackedDriver, const N: usize> {
    pub driver: D,
    pub blossom_tracker: BlossomTracker<N>,
}

impl<D: DualStacklessDriver + DualTrackedDriver, const N: usize> DualStacklessDriver for DualDriverTracked<D, N> {
    fn reset(&mut self) {
        self.driver.reset();
        self.blossom_tracker.clear();
    }

    fn set_speed(&mut self, is_blossom: bool, node: CompactNodeIndex, speed: CompactGrowState) {
        self.driver.set_speed(is_blossom, node, speed);
        if is_blossom {
            self.blossom_tracker.set_speed(node, speed);
        }
    }

    fn on_blossom_created(&mut self, blossom: CompactNodeIndex) {
        self.blossom_tracker.create_blossom(blossom);
    }

    fn on_blossom_expanded(&mut self, blossom: CompactNodeIndex) {
        self.blossom_tracker.set_speed(blossom, CompactGrowState::Stay);
    }

    fn on_blossom_absorbed_into_blossom(&mut self, child: CompactNodeIndex) {
        self.blossom_tracker.set_speed(child, CompactGrowState::Stay);
    }

    fn set_blossom(&mut self, node: CompactNodeIndex, blossom: CompactNodeIndex) {
        self.driver.set_blossom(node, blossom);
    }

    fn find_obstacle(&mut self) -> (CompactObstacle, CompactWeight) {
        let mut grown = 0;
        // Watchdog: if hardware keeps reporting GrowLength with zero or tiny local_grown
        // (can happen when primal/hardware diverge on archived blossoms in streaming mode),
        // bail out with `None` so the outer solve loop can terminate or hit its own
        // watchdog. Without this, this inner loop is infinite and bypasses that watchdog.
        const FIND_OBSTACLE_WATCHDOG: usize = 100_000;
        let mut iters = 0;
        loop {
            let maximum_growth = if let Some((length, blossom)) = self.blossom_tracker.get_maximum_growth() {
                if length == 0 {
                    return (CompactObstacle::BlossomNeedExpand { blossom }, grown);
                } else {
                    length
                }
            } else {
                CompactWeight::MAX
            };
            let (obstacle, local_grown) = self.driver.find_conflict(maximum_growth);
            self.blossom_tracker.advance_time(local_grown as CompactTimestamp);
            grown += local_grown;
            if !obstacle.is_finite_growth() {
                return (obstacle, grown);
            }
            iters += 1;
            if iters >= FIND_OBSTACLE_WATCHDOG {
                return (CompactObstacle::None, grown);
            }
        }
    }

    fn add_defect(&mut self, vertex: CompactVertexIndex, node: CompactNodeIndex) {
        self.driver.add_defect(vertex, node);
    }

    fn fuse_layer(&mut self, layer_id: CompactLayerNum) {
        self.driver.fuse_layer(layer_id);
    }

    fn archive_elastic_slice(&mut self) {
        self.driver.archive_elastic_slice();
        // Clear blossom tracker: the archive shifts all layer fusion vertex dual variables
        // in hardware, invalidating the software tracker's blossom checkpoints.
        //
        // After clearing, archived blossom shrinking is detected purely by the hardware scan
        // pipeline (archivedEdgeResponse reports remaining=0 as a conflict). The software
        // blossom tracker does NOT track archived blossoms — only live ones.
        //
        // This means: if an archived blossom shrinks, the hardware detects it via the scan
        // and reports it as a conflict in the convergecast. The primal module then resolves
        // it (e.g., expand the blossom). The hardware clamps grown to 0 on underflow
        // (VertexPostExecuteState), so no UInt wrap-around occurs.
        self.blossom_tracker.clear();
    }
}

impl<D: DualStacklessDriver + DualTrackedDriver, const N: usize> DualDriverTracked<D, N> {
    pub const fn new(driver: D) -> Self {
        Self {
            driver,
            blossom_tracker: BlossomTracker::new(),
        }
    }
}
