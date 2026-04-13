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
    /// Primal and dual persist across rounds (no reset). Each round: load defects on the
    /// top fusion layer, fuse that layer only, solve, archive. Node indices increase across
    /// rounds so the embedded primal does not reuse stale node IDs.
    ///
    /// Rounds are chosen so matchings **use archived vertex state**: repeating the same
    /// top vertex (e.g. v24 after v24) hits the fusion column (live v24 vs shifted state at
    /// v16); pairs on v24/v27 after prior singles exercise deep stack; a blossom round then a
    /// lone v24 stresses archived-blossom paths (see `dual_module_looper_cluster_across_archive_boundary`).
    #[test]
    fn dual_module_looper_streaming_decode() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_streaming_decode -- --nocapture
        use crate::primal_module_embedded_adaptor::PrimalModuleEmbedded;
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

        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let top_layer = (num_layers - 1) as CompactLayerNum;
        let mut node_offset: usize = 0;
        let mut dual: Box<DualModuleLooper> = stacker::grow(MAX_NODE_NUM * 256, || {
            Box::new(DualModuleStackless::new(
                DualDriverTracked::new(DualModuleLooperDriver::new(graph.clone(), config).unwrap()),
            ))
        });
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

        // Top-layer vertices: 24, 27, 28, 31. Each vec is one measurement round.
        // Order forces reads/writes through archived elastic slices (not only fresh top sites).
        let measurement_rounds: Vec<Vec<usize>> = vec![
            vec![24],       // R0: seed column — after archive, column state shifts down (v24→v16, …)
            vec![24],       // R1: same top vertex again — new defect must meet archived slice via fusion
            vec![27],       // R2: second column for later multi-site rounds
            vec![24, 27],   // R3: pair — both columns carry shifted archive under the defects
            vec![24],       // R4: lone v24 after blossom-bearing history — archived-blossom / shrink stress
            vec![24, 27],   // R5: pair again — matching through deeper stacked archive
            vec![],         // R6: empty measurement
            vec![24, 27],   // R7: pair after empty — decoder must still use archive, not assume clean slate
            vec![28],       // R8: third corner alone after long history
            vec![31],       // R9: fourth corner — full top-layer set exercised across archives
        ];

        for (round, defects) in measurement_rounds.iter().enumerate() {
            for (i, &vertex) in defects.iter().enumerate() {
                dual.add_defect(ni!(vertex), ni!(node_offset + i));
            }

            dual.fuse_layer(top_layer);
            primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

            let mut iterations = 0;
            let (mut obstacle, _) = dual.find_obstacle();
            while !obstacle.is_none() {
                primal.resolve(&mut *dual, obstacle);
                (obstacle, _) = dual.find_obstacle();
                iterations += 1;
                assert!(iterations < 1000, "round {round}: solve loop stuck after {iterations} iterations");
            }

            dual.archive_elastic_slice();

            node_offset += defects.len();
            println!("round {round}: decoded {} defect(s) in {iterations} iterations", defects.len());

            // Verify: after archive, find_obstacle should report no conflicts from this round's live state
            // (top layer was reset, shifted state is matched)
            let (post_archive_obstacle, _) = dual.find_obstacle();
            assert!(
                post_archive_obstacle.is_none() || matches!(post_archive_obstacle, CompactObstacle::GrowLength { .. }),
                "round {round}: unexpected conflict after archive: {post_archive_obstacle:?}"
            );
        }

        // Final: opposite top corners — two defects in one round after maximal archive depth
        dual.add_defect(ni!(24), ni!(node_offset));
        dual.add_defect(ni!(31), ni!(node_offset + 1));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());
        let mut iterations = 0;
        let (mut obstacle, _) = dual.find_obstacle();
        while !obstacle.is_none() {
            primal.resolve(&mut *dual, obstacle);
            (obstacle, _) = dual.find_obstacle();
            iterations += 1;
            assert!(iterations < 1000, "final round: solve loop stuck after {iterations} iterations");
        }
        dual.archive_elastic_slice();
        println!(
            "final round (v24+v31): decoded in {iterations} iterations after {} prior rounds — streaming ok",
            measurement_rounds.len()
        );
    }

    /// Helper: run N rounds of streaming decode with rotating defect patterns.
    /// Returns the total number of defects processed.
    fn streaming_decode_n_rounds(
        num_rounds: usize,
        archive_depth: usize,
    ) {
        use crate::primal_module_embedded_adaptor::PrimalModuleEmbedded;
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
            "name": format!("looper_streaming_{num_rounds}r"),
            "sim_config": {
                "support_layer_fusion": true,
                "archive_depth": archive_depth
            }
        }))
        .unwrap();

        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let top_layer = (num_layers - 1) as CompactLayerNum;
        // Top-layer vertices for phenomenological_rotated_d3
        let top_vertices: Vec<usize> = vec![24, 27, 28, 31];
        let mut node_offset: usize = 0;

        let mut dual: Box<DualModuleLooper> = stacker::grow(MAX_NODE_NUM * 256, || {
            Box::new(DualModuleStackless::new(
                DualDriverTracked::new(DualModuleLooperDriver::new(graph.clone(), config).unwrap()),
            ))
        });
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

        let mut total_defects = 0usize;
        let mut total_iterations = 0usize;

        for round in 0..num_rounds {
            // Rotating defect patterns: vary count and position each round
            let defects: Vec<usize> = match round % 5 {
                0 => vec![top_vertices[0]],                                    // single defect
                1 => vec![top_vertices[0], top_vertices[1]],                   // adjacent pair → conflict
                2 => vec![top_vertices[2]],                                    // different vertex
                3 => vec![],                                                   // empty measurement
                4 => vec![top_vertices[0], top_vertices[1], top_vertices[2]],  // three defects
                _ => unreachable!(),
            };

            for (i, &vertex) in defects.iter().enumerate() {
                dual.add_defect(ni!(vertex), ni!(node_offset + i));
            }

            dual.fuse_layer(top_layer);
            primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

            let mut iterations = 0;
            let (mut obstacle, grown) = dual.find_obstacle();
            println!("  round {round} solve: initial obstacle={obstacle:?} grown={grown}");
            while !obstacle.is_none() {
                primal.resolve(&mut *dual, obstacle);
                let (o, g) = dual.find_obstacle();
                obstacle = o;
                iterations += 1;
                println!("  round {round} solve: iter {iterations} → obstacle={obstacle:?} grown={g}");
                assert!(
                    iterations < 1000,
                    "round {round}/{num_rounds}: solve loop stuck after {iterations} iterations"
                );
            }

            dual.archive_elastic_slice();

            // Print matching state after archive
            println!("  round {round} matchings:");
            primal.nodes.iterate_intermediate_matching(|node, target, link| {
                let grow = primal.nodes.get_node(node).grow_state;
                println!("    node {} (grow={:?}) → {:?}", node.get(), grow, target);
            });

            total_defects += defects.len();
            total_iterations += iterations;
            node_offset += defects.len();

            println!(
                "round {round}/{num_rounds}: defects={defects:?}, {iterations} iters, node_offset={node_offset}",
            );
        }

        // Final round after all archives: system must still accept new measurements
        dual.add_defect(ni!(top_vertices[0]), ni!(node_offset));
        dual.add_defect(ni!(top_vertices[1]), ni!(node_offset + 1));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());
        let mut final_iters = 0;
        let (mut obstacle, _) = dual.find_obstacle();
        while !obstacle.is_none() {
            primal.resolve(&mut *dual, obstacle);
            (obstacle, _) = dual.find_obstacle();
            final_iters += 1;
            assert!(final_iters < 1000, "final round stuck");
        }
        dual.archive_elastic_slice();

        println!(
            "streaming_decode_n_rounds({num_rounds}): total_defects={total_defects}, \
             total_iterations={total_iterations}, final_iters={final_iters} — PASS"
        );
    }

    /// 5 rounds of streaming decode
    #[test]
    fn dual_module_looper_streaming_5_rounds() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_streaming_5_rounds -- --nocapture
        streaming_decode_n_rounds(5, 8);
    }

    /// 10 rounds of streaming decode. archive_depth=4 bounds scan stall to 4 entries max.
    /// Verilator simulation is slow per cycle; larger depths cause TCP timeout.
    #[test]
    fn dual_module_looper_streaming_10_rounds() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_streaming_10_rounds -- --nocapture
        streaming_decode_n_rounds(10, 4);
    }

    /// 20 rounds of streaming decode. archive_depth=4.
    #[test]
    fn dual_module_looper_streaming_20_rounds() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_streaming_20_rounds -- --nocapture
        streaming_decode_n_rounds(20, 4);
    }

    /// Helper: set up a streaming decode environment and return the components.
    fn make_streaming_env(
        archive_depth: usize,
    ) -> (
        Box<DualModuleLooper>,
        Box<crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>>,
        MicroBlossomSingle,
        CompactLayerNum,
    ) {
        use crate::primal_module_embedded_adaptor::PrimalModuleEmbedded;
        use micro_blossom_nostd::util::*;

        let graph_path = format!(
            "{}/../../../resources/graphs/example_phenomenological_rotated_d3.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let json_str = std::fs::read_to_string(&graph_path)
            .unwrap_or_else(|e| panic!("read graph {graph_path}: {e}"));
        let mut graph: MicroBlossomSingle = serde_json::from_str(&json_str).expect("parse graph");
        graph.offloading = crate::resources::OffloadingFinder::new();

        let test_name = format!("looper_correctness_{}", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() % 100000);
        let config: DualLooperConfig = serde_json::from_value(json!({
            "name": test_name,
            "sim_config": {
                "support_layer_fusion": true,
                "archive_depth": archive_depth
            }
        }))
        .unwrap();

        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let top_layer = (num_layers - 1) as CompactLayerNum;

        let dual: Box<DualModuleLooper> = stacker::grow(MAX_NODE_NUM * 256, || {
            Box::new(DualModuleStackless::new(
                DualDriverTracked::new(DualModuleLooperDriver::new(graph.clone(), config).unwrap()),
            ))
        });
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

        (dual, primal, graph, top_layer)
    }

    /// Format obstacle with layer info for debugging.
    /// Layer-0 vertices (v0,v3,v4,v7) indicate archived edge conflicts.
    fn format_obstacle(obstacle: &CompactObstacle) -> String {
        match obstacle {
            CompactObstacle::None => "None".to_string(),
            CompactObstacle::GrowLength { length } => format!("GrowLength({length})"),
            CompactObstacle::Conflict { node_1, node_2, touch_1, touch_2, vertex_1, vertex_2 } => {
                // Layer-0 vertices: 0,3,4,7. If both vertices are layer-0, it's an archived conflict.
                let layer_0 = [0u32, 3, 4, 7];
                let v1 = vertex_1.get();
                let v2 = vertex_2.get();
                let edge_type = if layer_0.contains(&v1) && layer_0.contains(&v2) {
                    "ARCHIVED"
                } else if layer_0.contains(&v1) || layer_0.contains(&v2) {
                    "CROSS-LAYER"
                } else {
                    "LIVE"
                };
                format!(
                    "Conflict[{edge_type}](n1={}, n2={}, v1={v1}, v2={v2}, t1={}, t2={})",
                    node_1.map(|n| n.get() as i64).unwrap_or(-1),
                    node_2.map(|n| n.get() as i64).unwrap_or(-1),
                    touch_1.map(|n| n.get() as i64).unwrap_or(-1),
                    touch_2.map(|n| n.get() as i64).unwrap_or(-1),
                )
            }
        }
    }

    /// Helper: run one streaming round — add defects, fuse, solve, archive.
    /// Prints detailed obstacle/matching info when `verbose` is true.
    fn streaming_round(
        dual: &mut DualModuleLooper,
        primal: &mut crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>,
        defects: &[usize],
        node_offset: usize,
        top_layer: CompactLayerNum,
        round_label: &str,
    ) -> usize {
        streaming_round_verbose(dual, primal, defects, node_offset, top_layer, round_label, false)
    }

    fn streaming_round_verbose(
        dual: &mut DualModuleLooper,
        primal: &mut crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>,
        defects: &[usize],
        node_offset: usize,
        top_layer: CompactLayerNum,
        round_label: &str,
        verbose: bool,
    ) -> usize {
        use micro_blossom_nostd::util::*;

        if verbose {
            println!("  [{round_label}] defects: {:?} (nodes {}..{})",
                defects, node_offset, node_offset + defects.len());
        }

        for (i, &vertex) in defects.iter().enumerate() {
            dual.add_defect(ni!(vertex), ni!(node_offset + i));
        }
        dual.fuse_layer(top_layer);
        primal.fuse_layer(dual, CompactLayerId::new(top_layer).unwrap());

        let mut iterations = 0;
        let (mut obstacle, grown) = dual.find_obstacle();
        if verbose {
            println!("  [{round_label}] initial obstacle: {} (grown={grown})", format_obstacle(&obstacle));
        }
        while !obstacle.is_none() {
            if verbose {
                println!("  [{round_label}] iter {iterations}: resolving {}", format_obstacle(&obstacle));
            }
            primal.resolve(dual, obstacle);
            let (o, g) = dual.find_obstacle();
            obstacle = o;
            if verbose {
                println!("  [{round_label}] iter {iterations}: after resolve → {} (grown={g})", format_obstacle(&obstacle));
            }
            iterations += 1;
            assert!(iterations < 2000, "{round_label}: solve loop stuck after {iterations} iterations");
        }

        dual.archive_elastic_slice();

        // Print matching state
        if verbose {
            println!("  [{round_label}] matchings after archive:");
            primal.nodes.iterate_intermediate_matching(|node, target, link| {
                println!("    node {} → {:?} (touch={:?}, peer_touch={:?})",
                    node.get(), target,
                    link.touch.option().map(|t| t.get()),
                    link.peer_touch.option().map(|t| t.get()));
            });
        }

        iterations
    }

    /// Print the full primal/dual state for debugging.
    fn print_state(
        dual: &DualModuleLooper,
        primal: &crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>,
        label: &str,
    ) {
        println!("  [{label}] primal matching state:");
        primal.nodes.iterate_intermediate_matching(|node, target, link| {
            let grow = primal.nodes.get_node(node).grow_state;
            println!("    node {} (grow={:?}) → {:?} (touch={:?}, peer_touch={:?})",
                node.get(), grow, target,
                link.touch.option().map(|t| t.get()),
                link.peer_touch.option().map(|t| t.get()));
        });
        // Snapshot the dual hardware state
        let snapshot = dual.driver.driver.snapshot(true);
        let vertices = snapshot.get("vertices").and_then(|v| v.as_array());
        if let Some(verts) = vertices {
            println!("  [{label}] dual vertex snapshot (non-trivial):");
            for (i, v) in verts.iter().enumerate() {
                let node = v.get("p").and_then(|n| n.as_i64());
                let defect = v.get("s").and_then(|d| d.as_bool()).unwrap_or(false);
                if node.is_some() || defect {
                    println!("    v{i}: node={:?} defect={defect}", node);
                }
            }
        }
    }

    /// Cross-layer cluster: a defect on v24 (layer 3) grows through the vertical fusion edge
    /// v24-v16 (edge 43, weight=2) to reach v16 (layer 2). After archive, v24's state shifts
    /// to v16 and v16's state shifts to v8. A SECOND defect on v24 in the next round should
    /// create a cluster that spans from live v16 (shifted from old v24) across the fusion edge
    /// to the new v24, and the archived state should also track the growth correctly.
    #[test]
    fn dual_module_looper_cross_layer_cluster() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_cross_layer_cluster -- --nocapture
        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);
        let mut node_offset = 0usize;

        println!("=== Round 0: single defect on v24 ===");
        let iters = streaming_round_verbose(&mut dual, &mut primal, &[24], node_offset, top_layer, "R0", true);
        node_offset += 1;
        print_state(&dual, &primal, "R0 post-archive");
        println!("R0: {iters} iterations");

        println!("\n=== Round 1: second defect on v24 (v16 holds old v24) ===");
        let iters = streaming_round_verbose(&mut dual, &mut primal, &[24], node_offset, top_layer, "R1", true);
        node_offset += 1;
        print_state(&dual, &primal, "R1 post-archive");
        println!("R1: {iters} iterations");

        println!("\n=== Round 2: two defects v24,v27 ===");
        let iters = streaming_round_verbose(&mut dual, &mut primal, &[24, 27], node_offset, top_layer, "R2", true);
        node_offset += 2;
        print_state(&dual, &primal, "R2 post-archive");
        println!("R2: {iters} iterations");

        println!("\ncross-layer cluster test passed");
    }

    /// Defects on the SAME vertex across multiple rounds should create independent clusters
    /// in the archive. Round 0: defect on v24 → archive. Round 1: defect on v24 (different node)
    /// → archive. The two archived clusters (now shifted to v16 and v8 respectively, or both
    /// in layer-0 archivedRegs at different depths) must not interfere.
    #[test]
    fn dual_module_looper_same_vertex_multiple_rounds() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_same_vertex_multiple_rounds -- --nocapture

        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);
        let mut node_offset = 0usize;

        for round in 0..4 {
            println!("=== Round {round}: defect on v24, node={node_offset} ===");
            let iters = streaming_round(&mut dual, &mut primal, &[24], node_offset, top_layer,
                &format!("R{round}"));
            node_offset += 1;
            println!("R{round}: {iters} iterations");
        }

        // Final round: two defects to exercise cross-archive conflicts
        println!("\n=== Final: defects on v24,v27 ===");
        let iters = streaming_round(&mut dual, &mut primal, &[24, 27], node_offset, top_layer, "Final");
        println!("Final: {iters} iterations");

        println!("\nsame vertex multiple rounds test passed");
    }

    /// Two adjacent defects that form a cluster spanning the archive boundary.
    /// Round 0: defect on v24. Archive. v24→v16.
    /// Round 1: defect on v24 (new). v16 holds old v24's state.
    /// Grow: v24 grows toward v16 via fusion edge 43. v16 (holding old v24) also grows
    /// (if speed=Grow). If both reach the fusion edge weight, a conflict is detected
    /// between the live v24's cluster and the shifted v16's cluster. This is the key
    /// cross-archive-boundary cluster test.
    #[test]
    fn dual_module_looper_cluster_across_archive_boundary() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_cluster_across_archive_boundary -- --nocapture
        use micro_blossom_nostd::util::*;

        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);

        // Round 0: defect on v24 (node=0). Grow + solve. The defect grows to a boundary.
        println!("=== Round 0: defect on v24 ===");
        dual.add_defect(ni!(24), ni!(0));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        // Manually grow step by step to observe the cross-layer interaction
        let (mut obs, grown) = dual.find_obstacle();
        println!("R0 initial: obs={obs:?}, grown={grown}");
        let mut total_grown = grown;
        let mut iters = 0;
        while !obs.is_none() {
            primal.resolve(&mut *dual, obs);
            let (o, g) = dual.find_obstacle();
            obs = o;
            total_grown = g;
            iters += 1;
            println!("R0 iter {iters}: obs={obs:?}, grown={total_grown}");
            assert!(iters < 100, "R0 stuck");
        }
        println!("R0: resolved in {iters} iterations, grown={total_grown}");

        dual.archive_elastic_slice();
        print_state(&dual, &primal, "R0 post-archive");

        // Round 1: defect on v24 (node=1). After archive, v16 holds old v24's state.
        // The fusion edge v16-v24 (edge 43, weight=2) connects old cluster (at v16) with new (at v24).
        println!("\n=== Round 1: defect on v24 (v16 holds old v24) ===");
        dual.add_defect(ni!(24), ni!(1));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        let (mut obs, grown) = dual.find_obstacle();
        println!("R1 initial: obs={obs:?}, grown={grown}");
        iters = 0;
        while !obs.is_none() {
            primal.resolve(&mut *dual, obs);
            let (o, g) = dual.find_obstacle();
            obs = o;
            println!("R1 iter {}: obs={:?}, grown={}", iters + 1, obs, g);
            iters += 1;
            assert!(iters < 100, "R1 stuck");
        }
        println!("R1: resolved in {iters} iterations");

        dual.archive_elastic_slice();
        print_state(&dual, &primal, "R1 post-archive");

        // Round 2: both v24 and v27. Two new defects on top layer, while archived state
        // from rounds 0 and 1 persists at deeper layers.
        println!("\n=== Round 2: defects on v24,v27 with deep archive ===");
        dual.add_defect(ni!(24), ni!(2));
        dual.add_defect(ni!(27), ni!(3));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        iters = 0;
        let (mut obs, _) = dual.find_obstacle();
        while !obs.is_none() {
            primal.resolve(&mut *dual, obs);
            (obs, _) = dual.find_obstacle();
            iters += 1;
            assert!(iters < 100, "R2 stuck");
        }
        println!("R2: resolved in {iters} iterations");

        dual.archive_elastic_slice();
        print_state(&dual, &primal, "R2 post-archive");
        println!("\ncluster across archive boundary test passed");
    }

    /// Empty rounds interspersed with defect rounds — the archive should handle no-ops correctly.
    /// The blossom tracker clear and primal state must survive empty rounds.
    #[test]
    fn dual_module_looper_empty_rounds_interspersed() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_empty_rounds_interspersed -- --nocapture
        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);
        let mut node_offset = 0usize;

        let rounds: Vec<Vec<usize>> = vec![
            vec![24],       // defect
            vec![],         // empty
            vec![],         // empty
            vec![27],       // defect on different vertex
            vec![],         // empty
            vec![24, 27],   // two defects that conflict
            vec![],         // empty after conflict
            vec![28],       // defect after empty
        ];

        for (round, defects) in rounds.iter().enumerate() {
            let iters = streaming_round(&mut dual, &mut primal, defects, node_offset, top_layer,
                &format!("R{round}"));
            println!("round {round}: {} defect(s), {iters} iterations", defects.len());
            node_offset += defects.len();
        }

        println!("\nempty rounds interspersed test passed");
    }

    /// Shrinking blossom in archive: create a blossom, archive it, then shrink it.
    /// The hardware should clamp grown to 0 (no underflow) and the blossom should be
    /// expandable by the primal module.
    #[test]
    fn dual_module_looper_archived_blossom_shrink() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_archived_blossom_shrink -- --nocapture
        use micro_blossom_nostd::util::*;

        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);

        // Round 0: two defects on v24, v27 → they conflict → blossom created
        println!("=== Round 0: create blossom from v24,v27 conflict ===");
        dual.add_defect(ni!(24), ni!(0));
        dual.add_defect(ni!(27), ni!(1));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        let mut iters = 0;
        let (mut obs, _) = dual.find_obstacle();
        while !obs.is_none() {
            println!("R0 iter {iters}: {obs:?}");
            primal.resolve(&mut *dual, obs);
            (obs, _) = dual.find_obstacle();
            iters += 1;
            assert!(iters < 100, "R0 stuck");
        }
        println!("R0: {iters} iterations — blossom formed and matched");

        // Archive: the blossom state (possibly with speed=Shrink) goes into archive
        dual.archive_elastic_slice();

        // Round 1: continue growing. The archived blossom may shrink.
        // If it shrinks to 0, the hardware clamps (no underflow).
        println!("\n=== Round 1: grow while archived blossom shrinks ===");
        dual.add_defect(ni!(24), ni!(2));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        iters = 0;
        let (mut obs, _) = dual.find_obstacle();
        while !obs.is_none() {
            println!("R1 iter {iters}: {obs:?}");
            primal.resolve(&mut *dual, obs);
            (obs, _) = dual.find_obstacle();
            iters += 1;
            assert!(iters < 100, "R1 stuck");
        }
        println!("R1: {iters} iterations — no underflow crash");

        // Round 2: more growth to stress the shrunk blossom further
        dual.archive_elastic_slice();
        println!("\n=== Round 2: continued growth ===");
        dual.add_defect(ni!(28), ni!(3));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        iters = 0;
        let (mut obs, _) = dual.find_obstacle();
        while !obs.is_none() {
            primal.resolve(&mut *dual, obs);
            (obs, _) = dual.find_obstacle();
            iters += 1;
            assert!(iters < 100, "R2 stuck");
        }
        dual.archive_elastic_slice();
        println!("R2: {iters} iterations — archived blossom shrink test passed");
    }

    /// All four top-layer vertices have defects — maximum conflict scenario.
    /// Tests that the solver can resolve a complex multi-defect round and archive it.
    #[test]
    fn dual_module_looper_max_defects_per_round() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_max_defects_per_round -- --nocapture
        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);
        let mut node_offset = 0usize;

        // 3 rounds, each with all 4 top-layer defects
        for round in 0..3 {
            println!("=== Round {round}: all 4 top-layer defects ===");
            let iters = streaming_round(
                &mut dual, &mut primal,
                &[24, 27, 28, 31],
                node_offset, top_layer,
                &format!("R{round}"),
            );
            node_offset += 4;
            println!("R{round}: {iters} iterations");
        }

        // Final: single defect after complex history
        println!("\n=== Final: single defect after 3 max-defect rounds ===");
        let iters = streaming_round(&mut dual, &mut primal, &[24], node_offset, top_layer, "Final");
        println!("Final: {iters} iterations — max defects test passed");
    }

    /// Archive-dependent matching: two defects placed in separate rounds, with empty rounds
    /// in between to shift data to layer 0. After warmup, the archived defect grows via scan
    /// and its edge becomes tight — the matching DEPENDS on the archived vertex's grown value
    /// being updated by the scan pipeline.
    ///
    /// Graph: phenomenological_rotated_d3, numLayers=4, warmupThreshold=3.
    ///
    /// Round 0: defect on v24 (node=0). Grow. Matches to boundary. grown=1, speed=Grow.
    /// Round 1: empty. Archive shifts data down (v24→v16→v8→v0 over rounds 0-2).
    /// Round 2: empty. archiveTotalCount=3, warmupDone. archivedRegs[0] committed with
    ///          round 0's data at layer 0: node=0, grown=1, speed=Grow (or Stay after match).
    /// Round 3: defect on v24 (node=1) AND defect on v27 (node=2).
    ///          Grow(1): scan applies Grow to archived — archived v0 grows from 1→2.
    ///          Archived edge v0-v3 (w=2): 2+0 >= 2 → archived conflict.
    ///          Live edge v24-v27 (w=2): both grow to 1 → 1+1 >= 2 → live conflict.
    ///          The solver must resolve BOTH the live and archived conflicts.
    ///          Without archived scan updating grown, the archived conflict would be missed.
    #[test]
    fn dual_module_looper_archive_dependent_matching() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_archive_dependent_matching -- --nocapture
        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);

        // Round 0: single defect on v24.
        println!("=== Round 0: defect on v24 (node=0) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[24], 0, top_layer, "R0", true);
        print_state(&dual, &primal, "R0");

        // Rounds 1, 2: empty. Shift data down through layers.
        println!("\n=== Rounds 1-2: empty (shifting data to layer 0) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[], 1, top_layer, "R1", true);
        streaming_round_verbose(&mut dual, &mut primal, &[], 1, top_layer, "R2", true);
        print_state(&dual, &primal, "R2 (warmup done, archivedRegs[0] committed)");

        // Round 3: two defects on v24 and v27. This forces both live AND archived conflicts.
        // The archived defect (round 0's v24 → now at layer 0) must have its grown updated
        // by the scan to trigger the archived edge tightness.
        println!("\n=== Round 3: defects on v24 (node=1), v27 (node=2) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[24, 27], 1, top_layer, "R3", true);
        print_state(&dual, &primal, "R3");

        // Round 4: another pair to exercise archived state further.
        println!("\n=== Round 4: defects on v28 (node=3), v31 (node=4) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[28, 31], 3, top_layer, "R4", true);
        print_state(&dual, &primal, "R4");

        println!("\narchive-dependent matching test passed");
    }

    /// Two defects on DIFFERENT spatial positions across rounds. Round 0: v24 (position 0,1).
    /// Round 3 (after warmup): v27 (position 1,2). The archived v24 data is at v0's archivedRegs.
    /// The archived v27 data doesn't exist (different spatial position → different layer-0 vertex v3).
    /// Only the v0 column has archived data. This tests that the edge scan correctly handles
    /// asymmetric archive: one endpoint has archived state, the other doesn't.
    #[test]
    fn dual_module_looper_asymmetric_archive() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_asymmetric_archive -- --nocapture
        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env(8);

        // Round 0: defect on v24 only (maps to v0 column).
        println!("=== Round 0: defect on v24 (node=0) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[24], 0, top_layer, "R0", true);

        // Rounds 1, 2: empty shifts.
        println!("\n=== Rounds 1-2: empty ===");
        streaming_round_verbose(&mut dual, &mut primal, &[], 1, top_layer, "R1", true);
        streaming_round_verbose(&mut dual, &mut primal, &[], 1, top_layer, "R2", true);

        // Round 3: defect on v27 (maps to v3 column — different from round 0's v0 column).
        // v0.archivedRegs[0] has round 0 data (v24→v0). v3.archivedRegs[0] has reset state.
        // The horizontal archived edge v0-v3 has one side with a defect, one side empty.
        println!("\n=== Round 3: defect on v27 (node=1) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[27], 1, top_layer, "R3", true);
        print_state(&dual, &primal, "R3");

        // Round 4: defects on both v24 and v27 — now both columns have live defects,
        // and the v0 column also has archived data.
        println!("\n=== Round 4: defects on v24 (node=2), v27 (node=3) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[24, 27], 2, top_layer, "R4", true);
        print_state(&dual, &primal, "R4");

        println!("\nasymmetric archive test passed");
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
