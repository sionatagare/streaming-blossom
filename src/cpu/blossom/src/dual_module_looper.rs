//! Dual Module implemented in Scala (SpinalHDL) with Stream interface and simulated via verilator
//!
//! This dual module will spawn a Scala program that compiles and runs a given decoding graph.
//! It simulates the complete MicroBlossomLooper module, which provides a stream interface.
//! (A wrapper around the DistributedDual module)
//!

use crate::mwpm_solver::*;
use crate::resources::*;
use crate::simulation_tcp_client::*;
use crate::util::*;
use fusion_blossom::dual_module::*;
use fusion_blossom::primal_module::*;
use fusion_blossom::visualize::*;
use micro_blossom_nostd::dual_driver_tracked::*;
use micro_blossom_nostd::dual_module_stackless::*;
use micro_blossom_nostd::instruction::*;
use micro_blossom_nostd::interface::*;
use micro_blossom_nostd::util::*;
use serde::*;

pub struct DualModuleLooperDriver {
    pub client: SimulationTcpClient,
    pub context_id: u16,
    pub instruction_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DualLooperConfig {
    #[serde(default = "Default::default")]
    pub sim_config: SimulationConfig,
    #[serde(default = "random_name_16")]
    pub name: String,
}

pub type DualModuleLooper = DualModuleStackless<DualDriverTracked<DualModuleLooperDriver, MAX_NODE_NUM>>;

impl SolverTrackedDual for DualModuleLooperDriver {
    fn new_from_graph_config(graph: MicroBlossomSingle, config: serde_json::Value) -> Self {
        Self::new(graph, serde_json::from_value(config).unwrap()).unwrap()
    }
    fn get_pre_matchings(&self, belonging: DualModuleInterfaceWeak) -> PerfectMatching {
        self.client.get_pre_matchings(belonging)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputData {
    pub instruction: u32, // use Instruction32
    pub context_id: u16,
    pub maximum_growth: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputData {
    pub context_id: u16,
    pub max_growable: u16,
    pub conflict: ConvergecastConflict,
    pub grown: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConvergecastConflict {
    pub node1: u16,
    pub node2: Option<u16>,
    pub touch1: u16,
    pub touch2: Option<u16>,
    pub vertex1: u16,
    pub vertex2: u16,
    pub valid: bool,
}

impl DualModuleLooperDriver {
    pub fn new(micro_blossom: MicroBlossomSingle, config: DualLooperConfig) -> std::io::Result<Self> {
        let mut value = Self {
            client: SimulationTcpClient::new("LooperHost", micro_blossom, config.name, config.sim_config)?,
            context_id: 0,
            instruction_count: 0,
        };
        value.reset();
        Ok(value)
    }

    fn execute(&mut self, input: InputData) -> std::io::Result<OutputData> {
        let json_str = serde_json::to_string(&input)?;
        let line = self.client.read_line(format!("execute: {json_str}"))?;
        self.instruction_count += 1;
        Ok(serde_json::from_str(line.as_str())?)
    }

    pub fn execute_instruction(&mut self, instruction: Instruction32, context_id: u16) -> std::io::Result<OutputData> {
        self.execute(InputData {
            instruction: instruction.into(),
            context_id,
            maximum_growth: 0,
        })
    }

    pub fn execute_find_obstacle(
        &mut self,
        context_id: u16,
        maximum_growth: u16,
    ) -> std::io::Result<(CompactObstacle, CompactWeight)> {
        let output = self.execute(InputData {
            instruction: Instruction32::find_obstacle().into(),
            context_id,
            maximum_growth,
        })?;
        let grown = CompactWeight::try_from(output.grown).unwrap();
        if output.max_growable == u16::MAX {
            assert!(!output.conflict.valid, "growable must be finite when conflict is detected");
            return Ok((CompactObstacle::None, grown));
        }
        if output.conflict.valid {
            return Ok((
                CompactObstacle::Conflict {
                    node_1: ni!(output.conflict.node1).option(),
                    node_2: output.conflict.node2.map(|v| ni!(v)).into(),
                    touch_1: ni!(output.conflict.touch1).option(),
                    touch_2: output.conflict.touch2.map(|v| ni!(v)).into(),
                    vertex_1: ni!(output.conflict.vertex1),
                    vertex_2: ni!(output.conflict.vertex2),
                },
                grown,
            ));
        }
        return Ok((
            CompactObstacle::GrowLength {
                length: CompactWeight::try_from(output.max_growable).unwrap(),
            },
            grown,
        ));
    }
}

impl DualStacklessDriver for DualModuleLooperDriver {
    fn reset(&mut self) {
        self.execute_instruction(Instruction32::reset(), self.context_id).unwrap();
        self.instruction_count = 0;
    }
    fn set_speed(&mut self, _is_blossom: bool, node: CompactNodeIndex, speed: CompactGrowState) {
        self.execute_instruction(Instruction32::set_speed(node, speed), self.context_id)
            .unwrap();
    }
    fn set_blossom(&mut self, node: CompactNodeIndex, blossom: CompactNodeIndex) {
        self.execute_instruction(Instruction32::set_blossom(node, blossom), self.context_id)
            .unwrap();
    }
    fn find_obstacle(&mut self) -> (CompactObstacle, CompactWeight) {
        self.execute_find_obstacle(self.context_id, u16::MAX).unwrap()
    }
    fn add_defect(&mut self, vertex: CompactVertexIndex, node: CompactNodeIndex) {
        self.execute_instruction(Instruction32::add_defect_vertex(vertex, node), self.context_id)
            .unwrap();
    }
    fn fuse_layer(&mut self, layer_id: CompactLayerNum) {
        self.execute_instruction(Instruction32::load_syndrome_external(ni!(layer_id)), self.context_id)
            .unwrap();
    }
    fn archive_elastic_slice(&mut self) {
        self.execute_instruction(Instruction32::archive_elastic_slice(), self.context_id)
            .unwrap();
    }
}

impl DualTrackedDriver for DualModuleLooperDriver {
    fn find_conflict(&mut self, maximum_growth: CompactWeight) -> (CompactObstacle, CompactWeight) {
        self.execute_find_obstacle(self.context_id, maximum_growth as u16).unwrap()
    }
}

impl FusionVisualizer for DualModuleLooperDriver {
    fn snapshot(&self, abbrev: bool) -> serde_json::Value {
        self.client.snapshot(abbrev)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dual_module_adaptor::tests::*;
    use fusion_blossom::util::*;
    use serde_json::json;

    // to use visualization, we need the folder of fusion-blossom repo
    // e.g. export FUSION_DIR=/Users/wuyue/Documents/GitHub/fusion-blossom

    #[test]
    fn dual_module_looper_basic_1() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_basic_1 -- --nocapture
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 BROADCAST_DELAY=2 cargo test dual_module_looper_basic_1 -- --nocapture
        let visualize_filename = "dual_module_looper_basic_1.json".to_string();
        let defect_vertices = vec![0, 4, 8];
        dual_module_looper_basic_standard_syndrome(3, visualize_filename, defect_vertices, json!({}));
    }

    /// test pre-matching
    #[test]
    fn dual_module_looper_basic_2() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_basic_2 -- --nocapture
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 BROADCAST_DELAY=2 cargo test dual_module_looper_basic_2 -- --nocapture
        let visualize_filename = "dual_module_looper_basic_2.json".to_string();
        let defect_vertices = vec![0, 4];
        let config = json!({ "support_offloading": true });
        dual_module_looper_basic_standard_syndrome(3, visualize_filename, defect_vertices, config);
    }

    /// test layer fusion
    #[test]
    fn dual_module_looper_basic_3() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_basic_3 -- --nocapture
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 BROADCAST_DELAY=2 cargo test dual_module_looper_basic_3 -- --nocapture
        let visualize_filename = "dual_module_looper_basic_3.json".to_string();
        let defect_vertices = vec![4, 8];
        let config = json!({ "support_layer_fusion": true });
        dual_module_looper_basic_standard_syndrome(3, visualize_filename, defect_vertices, config);
    }

    /// test layer fusion + pre-matching
    #[test]
    fn dual_module_looper_basic_4() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_basic_4 -- --nocapture
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 BROADCAST_DELAY=2 cargo test dual_module_looper_basic_4 -- --nocapture
        let visualize_filename = "dual_module_looper_basic_4.json".to_string();
        let defect_vertices = vec![4, 8];
        let config = json!({ "support_layer_fusion": true, "support_offloading": true });
        dual_module_looper_basic_standard_syndrome(3, visualize_filename, defect_vertices, config);
    }

    /// bug 2024.5.25: the grow value should not increase when isStalled is asserted
    #[test]
    fn dual_module_looper_debug_1() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_debug_1 -- --nocapture
        let visualize_filename = "dual_module_looper_debug_1.json".to_string();
        let defect_vertices = vec![1, 5, 8];
        let config = json!({ "support_offloading": true });
        dual_module_looper_basic_standard_syndrome(3, visualize_filename, defect_vertices, config);
    }

    /// Streaming decode: multiple measurement rounds arrive one at a time.
    /// Each round: load defects on top layer, fuse, solve to completion, archive.
    /// The primal module persists across rounds (no reset). After all rounds, the
    /// CPU must still be in a consistent state and the decoder must not panic or stall.
    #[test]
    fn dual_module_looper_streaming_decode() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_streaming_decode -- --nocapture
        use micro_blossom_nostd::primal_module_embedded::*;
        use micro_blossom_nostd::util::*;

        let graph_path = format!(
            "{}/../../../resources/graphs/example_phenomenological_rotated_d3.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let json_str = std::fs::read_to_string(&graph_path)
            .unwrap_or_else(|e| panic!("read graph {graph_path}: {e}"));
        let mut graph: MicroBlossomSingle = serde_json::from_str(&json_str).expect("parse graph");
        graph.offloading = crate::resources::OffloadingFinder::new();

        let config: DualLooperConfig = serde_json::from_value(json!({
            "name": "looper_streaming_decode",
            "sim_config": { "support_layer_fusion": true }
        }))
        .unwrap();

        let mut dual: DualModuleLooper = DualModuleStackless::new(
            DualDriverTracked::new(DualModuleLooperDriver::new(graph.clone(), config).unwrap()),
        );
        let mut primal: Box<PrimalModuleEmbedded<MAX_NODE_NUM>> =
            stacker::grow(MAX_NODE_NUM * 256, || Box::new(PrimalModuleEmbedded::new()));
        primal.nodes.blossom_begin = graph.vertex_num;
        if let Some(lf) = graph.layer_fusion.as_ref() {
            for vertex_index in 0..graph.vertex_num {
                if let Some(&layer_id) = lf.vertex_layer_id.get(&vertex_index) {
                    primal.layer_fusion.vertex_layer_id[vertex_index] =
                        CompactLayerId::new(layer_id as CompactLayerNum);
                }
            }
        }

        // Each inner vec is one measurement round's defect vertices (top-layer vertices).
        // Node indices are assigned incrementally across rounds.
        let measurement_rounds: Vec<Vec<usize>> = vec![
            vec![24],       // round 0: single defect
            vec![27],       // round 1: different vertex
            vec![24, 27],   // round 2: two defects — will produce a conflict
            vec![],         // round 3: empty measurement (no defects)
            vec![28],       // round 4: another round after empty — CPU must keep going
        ];

        let top_layer: CompactLayerNum = 3;
        let mut node_offset: usize = 0;

        for (round, defects) in measurement_rounds.iter().enumerate() {
            // Load this round's defects
            for (i, &vertex) in defects.iter().enumerate() {
                dual.add_defect(ni!(vertex), ni!(node_offset + i));
            }

            // Fuse the top layer (clears isVirtual, enables those vertices)
            dual.fuse_layer(top_layer);
            primal.fuse_layer(&mut dual, CompactLayerId::new(top_layer).unwrap());

            // Solve: find and resolve obstacles until none remain
            let (mut obstacle, _) = dual.find_obstacle();
            while !obstacle.is_none() {
                primal.resolve(&mut dual, obstacle);
                (obstacle, _) = dual.find_obstacle();
            }

            // Archive: shift state down, reset top layer for next measurement
            dual.archive_elastic_slice();

            node_offset += defects.len();
            println!("round {round}: decoded {} defect(s), total nodes so far: {node_offset}", defects.len());
        }

        // Final check: the system is still alive — issue one more round after all the above
        dual.add_defect(ni!(24), ni!(node_offset));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut dual, CompactLayerId::new(top_layer).unwrap());
        let (mut obstacle, _) = dual.find_obstacle();
        while !obstacle.is_none() {
            primal.resolve(&mut dual, obstacle);
            (obstacle, _) = dual.find_obstacle();
        }
        dual.archive_elastic_slice();
        println!("final round: decoded successfully after {} prior rounds — streaming decode ok", measurement_rounds.len());
    }

    pub fn dual_module_looper_basic_standard_syndrome(
        d: VertexNum,
        visualize_filename: String,
        defect_vertices: Vec<VertexIndex>,
        sim_config: serde_json::Value,
    ) -> SolverEmbeddedLooper {
        dual_module_standard_optional_viz(
            d,
            Some(visualize_filename.clone()),
            defect_vertices,
            |initializer, positions| {
                SolverEmbeddedLooper::new(
                    MicroBlossomSingle::new(initializer, positions),
                    json!({
                        "dual": {
                            "name": visualize_filename.as_str().trim_end_matches(".json").to_string(),
                            "sim_config": sim_config,
                            // "with_max_iterations": 30, // this is helpful when debugging infinite loops
                        }
                    }),
                )
            },
        )
    }
}
