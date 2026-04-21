use crate::binding::*;
use crate::binding::extern_c::*;
use cty::*;
use micro_blossom_nostd::dual_driver_tracked::*;
use micro_blossom_nostd::dual_module_stackless::*;
use micro_blossom_nostd::instruction::*;
use micro_blossom_nostd::interface::*;
use micro_blossom_nostd::util::*;

pub struct DualDriver {
    pub context_id: uint16_t,
}

impl DualDriver {
    pub const fn new() -> Self {
        Self { context_id: 0 }
    }
}

/// When INSTR_DBG is set, print every instruction issued with a sequence number.
/// Lets us see exactly which instruction call hangs when the hardware deadlocks.
pub const INSTR_DBG: bool = option_env!("INSTR_DBG").is_some();
static mut INSTR_SEQ: u32 = 0;
static mut INSTR_DBG_ENABLE: bool = false;
pub fn instr_dbg_set_enabled(enabled: bool) {
    unsafe { INSTR_DBG_ENABLE = enabled; }
}
fn dbg_tag(tag: &str, value: u32) {
    if INSTR_DBG && unsafe { INSTR_DBG_ENABLE } {
        let seq = unsafe {
            INSTR_SEQ = INSTR_SEQ.wrapping_add(1);
            INSTR_SEQ
        };
        println!("[instr_dbg] seq={seq} op={tag} arg={value:#x}");
    }
}

impl DualStacklessDriver for DualDriver {
    fn reset(&mut self) {
        dbg_tag("reset", 0);
        unsafe { execute_instruction(Instruction32::reset().0, self.context_id) };
        dbg_tag("reset_done", 0);
    }
    fn set_speed(&mut self, _is_blossom: bool, node: CompactNodeIndex, speed: CompactGrowState) {
        dbg_tag("set_speed", node.get() as u32 | ((speed as u32) << 16));
        unsafe { execute_instruction(Instruction32::set_speed(node, speed).0, self.context_id) };
        dbg_tag("set_speed_done", node.get() as u32);
    }
    fn set_blossom(&mut self, node: CompactNodeIndex, blossom: CompactNodeIndex) {
        dbg_tag("set_blossom", node.get() as u32 | ((blossom.get() as u32) << 16));
        unsafe { execute_instruction(Instruction32::set_blossom(node, blossom).0, self.context_id) };
        dbg_tag("set_blossom_done", node.get() as u32);
    }
    fn find_obstacle(&mut self) -> (CompactObstacle, CompactWeight) {
        dbg_tag("find_obstacle", 0);
        let r = unsafe { get_single_readout(self.context_id) }.into_obstacle();
        dbg_tag("find_obstacle_done", 0);
        r
    }
    fn add_defect(&mut self, vertex: CompactVertexIndex, node: CompactNodeIndex) {
        dbg_tag("add_defect", vertex.get() as u32 | ((node.get() as u32) << 16));
        unsafe { execute_instruction(Instruction32::add_defect_vertex(vertex, node).0, self.context_id) };
        dbg_tag("add_defect_done", vertex.get() as u32);
    }
    fn fuse_layer(&mut self, layer_id: CompactLayerNum) {
        dbg_tag("fuse_layer", layer_id as u32);
        unsafe { execute_instruction(Instruction32::load_syndrome_external(ni!(layer_id)).0, self.context_id) };
        dbg_tag("fuse_layer_done", layer_id as u32);
    }
    fn archive_elastic_slice(&mut self) {
        dbg_tag("archive", 0);
        unsafe { execute_instruction(Instruction32::archive_elastic_slice().0, self.context_id) };
        dbg_tag("archive_done", 0);
    }
}

impl DualTrackedDriver for DualDriver {
    fn find_conflict(&mut self, maximum_growth: CompactWeight) -> (CompactObstacle, CompactWeight) {
        dbg_tag("set_max_growth", maximum_growth as u32);
        unsafe { set_maximum_growth(maximum_growth as u16, self.context_id) };
        dbg_tag("set_max_growth_done", maximum_growth as u32);
        self.find_obstacle()
    }
}
