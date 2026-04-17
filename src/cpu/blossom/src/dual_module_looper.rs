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
    use crate::dual_module_comb::{DualCombConfig, DualModuleComb, DualModuleCombDriver};
    use fusion_blossom::util::*;
    use micro_blossom_nostd::interface::DualInterface;
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

    /// Evaluation-style check (benchmark `--verifier fusion-serial`): after Verilator decode, the
    /// merged MWPM (embedded primal + `pre_matchings()` from hardware) must match optimal weight
    /// and reproduce the syndrome.
    #[test]
    fn dual_module_looper_verify_merged_matching_vs_fusion_serial() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_verify_merged_matching_vs_fusion_serial -- --nocapture
        use crate::dual_module_adaptor::tests::dual_module_standard_optional_viz;

        dual_module_standard_optional_viz(
            3,
            None,
            vec![0, 4],
            |initializer, positions| {
                SolverEmbeddedLooper::new(
                    MicroBlossomSingle::new(initializer, positions),
                    json!({
                        "dual": {
                            "name": "dual_module_looper_verify_merged_matching_vs_fusion_serial",
                            "sim_config": { "support_offloading": true },
                        }
                    }),
                )
            },
        );
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

            archive_streaming_slice(&mut *dual);

            node_offset += defects.len();
            println!("round {round}: decoded {} defect(s) in {iterations} iterations", defects.len());

            // Do not assert post-archive quiescence here; the next round starts with fuse_layer and solve.
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
        archive_streaming_slice(&mut *dual);
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

            archive_streaming_slice(&mut *dual);

            // Print matching state after archive
            println!("  round {round} matchings:");
            primal.nodes.iterate_intermediate_matching(|node, target, _link| {
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
        archive_streaming_slice(&mut *dual);

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
    /// Uses d=3 phenomenological graph by default.
    fn make_streaming_env(
        archive_depth: usize,
    ) -> (
        Box<DualModuleLooper>,
        Box<crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>>,
        MicroBlossomSingle,
        CompactLayerNum,
    ) {
        make_streaming_env_custom(3, 3, archive_depth)
    }

    /// Helper: set up a streaming decode environment with custom graph parameters.
    /// `d` = code distance, `noisy_measurements` = number of noisy measurement rounds
    /// (num_layers = noisy_measurements + 1).
    fn make_streaming_env_custom(
        d: usize,
        noisy_measurements: usize,
        archive_depth: usize,
    ) -> (
        Box<DualModuleLooper>,
        Box<crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>>,
        MicroBlossomSingle,
        CompactLayerNum,
    ) {
        use crate::primal_module_embedded_adaptor::PrimalModuleEmbedded;
        use fusion_blossom::example_codes::*;
        use micro_blossom_nostd::util::*;

        let code = PhenomenologicalRotatedCode::new(d, noisy_measurements, 0.1, 1);
        let mut graph = MicroBlossomSingle::new_code(&code);
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

    /// Like [`make_streaming_env_custom`](Self::make_streaming_env_custom), but uses a pre-built
    /// [`MicroBlossomSingle`](MicroBlossomSingle) (e.g. custom weights from
    /// [`MicroBlossomSingle::phenomenological_rotated_d3_boundary7_spatial1`](MicroBlossomSingle::phenomenological_rotated_d3_boundary7_spatial1)).
    fn make_streaming_env_from_graph(
        mut graph: MicroBlossomSingle,
        archive_depth: usize,
    ) -> (
        Box<DualModuleLooper>,
        Box<crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>>,
        MicroBlossomSingle,
        CompactLayerNum,
    ) {
        use crate::primal_module_embedded_adaptor::PrimalModuleEmbedded;
        use micro_blossom_nostd::util::*;

        graph.offloading = crate::resources::OffloadingFinder::new();

        let test_name = format!(
            "looper_correctness_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
                % 100000
        );
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

    /// Same as [`make_streaming_env_from_graph`](Self::make_streaming_env_from_graph) but uses the
    /// Rust combinatorial dual ([`DualModuleComb`](crate::dual_module_comb::DualModuleComb)) as a
    /// reference for streaming + archive behaviour (no Scala / Verilator).
    fn make_streaming_comb_from_graph(
        mut graph: MicroBlossomSingle,
        archive_depth: usize,
    ) -> (
        Box<DualModuleComb>,
        Box<crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>>,
        MicroBlossomSingle,
        CompactLayerNum,
    ) {
        use crate::primal_module_embedded_adaptor::PrimalModuleEmbedded;
        use micro_blossom_nostd::util::*;

        graph.offloading = crate::resources::OffloadingFinder::new();

        let config: DualCombConfig = serde_json::from_value(json!({
            "sim_config": {
                "support_layer_fusion": true,
                "archive_depth": archive_depth
            }
        }))
        .unwrap();

        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let top_layer = (num_layers - 1) as CompactLayerNum;

        let dual: Box<DualModuleComb> = stacker::grow(MAX_NODE_NUM * 256, || {
            Box::new(DualModuleStackless::new(DualDriverTracked::new(DualModuleCombDriver::new(
                graph.clone(),
                config,
            ))))
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

    fn archive_streaming_slice(dual: &mut impl DualInterface) {
        dual.archive_elastic_slice();
    }

    /// Format obstacle with layer info for debugging.
    /// Layer-0 vertices (v0,v3,v4,v7) indicate archived edge conflicts.
    fn format_obstacle(obstacle: &CompactObstacle) -> String {
        match obstacle {
            CompactObstacle::None => "None".to_string(),
            CompactObstacle::GrowLength { length } => format!("GrowLength({length})"),
            CompactObstacle::BlossomNeedExpand { blossom } => format!("BlossomNeedExpand({})", blossom.get()),
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
                    node_1.option().map(|n| n.get() as i64).unwrap_or(-1),
                    node_2.option().map(|n| n.get() as i64).unwrap_or(-1),
                    touch_1.option().map(|n| n.get() as i64).unwrap_or(-1),
                    touch_2.option().map(|n| n.get() as i64).unwrap_or(-1),
                )
            }
        }
    }

    /// Helper: run one streaming round — add defects, fuse top layer, solve, archive.
    fn streaming_round(
        dual: &mut impl DualInterface,
        primal: &mut crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>,
        defects: &[usize],
        node_offset: usize,
        top_layer: CompactLayerNum,
        round_label: &str,
    ) -> usize {
        streaming_round_verbose(dual, primal, defects, node_offset, top_layer, round_label, false)
    }

    fn streaming_round_verbose(
        dual: &mut impl DualInterface,
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

        archive_streaming_slice(dual);

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

    fn print_dual_vertex_columns(dual: &DualModuleLooper, label: &str, vertex_indices: &[usize]) {
        let snapshot = dual.driver.driver.snapshot(true);
        let vertices = snapshot
            .get("vertices")
            .and_then(|value| value.as_array())
            .expect("snapshot vertices");
        println!("  [{label}] dual archived/live columns:");
        for &vertex_index in vertex_indices {
            let vertex = &vertices[vertex_index];
            let node = vertex.get("p").and_then(|value| value.as_i64());
            let root = vertex.get("pg").and_then(|value| value.as_i64());
            let defect = vertex.get("s").and_then(|value| value.as_bool()).unwrap_or(false);
            let archived = vertex
                .get("a")
                .and_then(|value| value.as_array())
                .map(|states| {
                    states
                        .iter()
                        .enumerate()
                        .map(|(index, state)| {
                            format!(
                                "#{index}(node={:?},root={:?},grown={},speed={},virt={},defect={})",
                                state.get("p").and_then(|value| value.as_i64()),
                                state.get("pg").and_then(|value| value.as_i64()),
                                state.get("g").and_then(|value| value.as_i64()).unwrap_or(-1),
                                state.get("sp").and_then(|value| value.as_i64()).unwrap_or(-1),
                                state.get("v").and_then(|value| value.as_bool()).unwrap_or(false),
                                state.get("s").and_then(|value| value.as_bool()).unwrap_or(false),
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" | ")
                })
                .unwrap_or_else(|| "(no archive)".to_string());
            println!(
                "    v{vertex_index}: live(node={node:?}, root={root:?}, defect={defect}) archived=[{archived}]"
            );
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
        let iters = streaming_round_verbose(
            &mut dual,
            &mut primal,
            &[24, 27],
            node_offset,
            top_layer,
            "R2",
            true,
        );
        let _ = node_offset;
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

        archive_streaming_slice(&mut *dual);
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

        archive_streaming_slice(&mut *dual);
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

        archive_streaming_slice(&mut *dual);
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
        archive_streaming_slice(&mut *dual);

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
        archive_streaming_slice(&mut *dual);
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
        archive_streaming_slice(&mut *dual);
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

    /// Archive–live pairing along one fusion column after **hardware archive warmup**.
    ///
    /// Round 0: interior top vertex, node 0 — **decode to quiescence first**, then
    /// `archive_streaming_slice` (`ArchiveElasticSlice` after a quiescent solve).
    ///
    /// Next `num_layers - 1` rounds: **empty** syndromes (`fuse_layer` → solve → archive each).
    /// On a **2-layer** graph that is one round, completing `warmupDone` while keeping a stable
    /// final **peer** `0 ↔ 1` on the same top site (`noisy_measurements=1`).
    ///
    /// Final round: same top vertex with node 1. **Peer** `0 ↔ 1` is printed when it occurs; with
    /// solve-first R0 the MWPM optimum is often two virtuals instead (see log `NOTE:`).
    ///
    /// Graph: phenomenological rotated, d=5, `noisy_measurements=1` → 2 layers, `archive_depth=8`.
    #[test]
    fn dual_module_looper_archive_dependent_matching() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_archive_dependent_matching -- --nocapture
        use micro_blossom_nostd::util::*;

        // d=5, noisy_measurements=1 → 2 layers, warmup = num_layers - 1 = 1 (stable peer 0↔1).
        let (mut dual, mut primal, graph, top_layer) = make_streaming_env_custom(5, 1, 8);
        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let warmup_rounds = num_layers.saturating_sub(1);
        let top_verts: Vec<usize> = graph.layer_fusion.as_ref().unwrap().layers
            .last().unwrap().iter().map(|v| *v as usize).collect();
        let virt: std::collections::HashSet<usize> = graph.virtual_vertices.iter().map(|v| *v as usize).collect();

        // Find an interior top-layer vertex (no direct boundary neighbor)
        let interior_v = top_verts.iter().copied().find(|&v| {
            !graph.weighted_edges.iter().any(|e| {
                (e.l as usize == v && virt.contains(&(e.r as usize))) ||
                (e.r as usize == v && virt.contains(&(e.l as usize)))
            })
        }).expect("no interior vertex found");
        println!(
            "Using interior vertex v{interior_v} (d=5, {num_layers} layers, warmup={warmup_rounds})"
        );

        // Round 0: decode to quiescence, then archive.
        println!("\n=== Round 0: defect on v{interior_v} (node=0), solve then archive ===");
        streaming_round_verbose(
            &mut dual,
            &mut primal,
            &[interior_v],
            0,
            top_layer,
            "R0",
            true,
        );
        print_state(&dual, &primal, "R0");

        // Empty rounds: complete warmup and shift column state into archivedRegs.
        println!("\n=== Rounds 1..{warmup_rounds}: empty syndromes (warmup + shift) ===");
        for i in 0..warmup_rounds {
            streaming_round_verbose(
                &mut dual,
                &mut primal,
                &[],
                1,
                top_layer,
                &format!("R_empty_{i}"),
                true,
            );
            print_state(&dual, &primal, &format!("R_empty_{i}"));
        }

        // Final: new defect same spatial site (node 1); must pair with prior round's node 0.
        println!("\n=== Final: defect on v{interior_v} (node=1) — expect peer 0↔1 ===");
        streaming_round_verbose(&mut dual, &mut primal, &[interior_v], 1, top_layer, "R_final", true);
        print_state(&dual, &primal, "R_final");

        println!("\nFinal matchings:");
        let mut matchings = vec![];
        primal.nodes.iterate_intermediate_matching(|node, target, _link| {
            let grow = primal.nodes.get_node(node).grow_state;
            println!("  node {} (grow={:?}) → {:?}", node.get(), grow, target);
            matchings.push((node.get(), target));
        });

        let peer_matched = matchings.iter().any(|(n, t)| {
            *n == 0 && matches!(t, CompactMatchTarget::Peer(p) if p.get() == 1)
        });
        if peer_matched {
            println!("  peer 0↔1 (time-like) — same optimum as unsolved-R0 variant on this graph.");
        } else {
            println!(
                "  NOTE: solve-first R0 often yields spatial/temporal virtuals instead of peer 0↔1; \
                 matchings={matchings:?}."
            );
        }

        println!("\narchive-dependent matching test passed");
    }

    /// One-line summary of outer matched nodes after a streaming round (for `boundary7` test logs).
    fn print_outer_matching_line(
        primal: &crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>,
        tag: &str,
    ) {
        let mut parts = Vec::new();
        primal.nodes.iterate_intermediate_matching(|node, target, _link| {
            parts.push(format!("n{}→{:?}", node.get(), target));
        });
        println!(
            "  [summary {tag}] outer matchings ({}): {}",
            parts.len(),
            if parts.is_empty() {
                "(none)".to_string()
            } else {
                parts.join(" | ")
            }
        );
    }

    fn collect_outer_matchings(
        primal: &crate::primal_module_embedded_adaptor::PrimalModuleEmbedded<MAX_NODE_NUM>,
    ) -> Vec<(u32, CompactMatchTarget)> {
        let mut matchings = Vec::new();
        primal.nodes.iterate_intermediate_matching(|node, target, _link| {
            matchings.push((node.get(), target));
        });
        matchings
    }

    fn assert_peer_pair(matchings: &[(u32, CompactMatchTarget)], node_a: u32, node_b: u32) {
        assert!(
            matchings.iter().any(|(node, target)| {
                (*node == node_a && matches!(target, CompactMatchTarget::Peer(peer) if peer.get() == node_b))
                    || (*node == node_b
                        && matches!(target, CompactMatchTarget::Peer(peer) if peer.get() == node_a))
            }),
            "expected peer pair ({node_a}, {node_b}), got matchings={matchings:?}"
        );
    }

    fn assert_virtual_matching(matchings: &[(u32, CompactMatchTarget)], node_index: u32) {
        assert!(
            matchings
                .iter()
                .any(|(node, target)| *node == node_index && matches!(target, CompactMatchTarget::VirtualVertex(_))),
            "expected node {node_index} virtual-matched, got matchings={matchings:?}"
        );
    }

    /// Custom-weight phenomenological d=3 graph ([`MicroBlossomSingle::phenomenological_rotated_d3_archive_v24_v27_then_v27`]):
    /// base real-real edges **6**, boundary edges **12**, with a forced cheap path
    /// `v24-v27` and `v27-v19-v11-v3`, plus expensive `v0-boundary` (250).
    ///
    /// **R0:** defects on top **v24** and **v27** (nodes 0 and 1), decode, archive.
    ///
    /// **Warmup:** `num_layers - 1` empty streaming rounds (same pattern as hardware `warmupDone`),
    /// each fuse → trivial solve → archive, so the v24/v27 state moves into the elastic archive.
    ///
    /// **Final:** a single new defect on **v27** (node 2). For the current fused graph weights,
    /// both the combinatorial dual and the Scala looper converge to the **same** optimum: keep the
    /// archived `0↔1` peer and **virtual-match** live node `2` (see
    /// [`dual_module_looper_boundary7_v24_v27_warmup_then_v27_comb_reference`](Self::dual_module_looper_boundary7_v24_v27_warmup_then_v27_comb_reference)).
    #[test]
    fn dual_module_looper_boundary7_v24_v27_warmup_then_v27() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_boundary7_v24_v27_warmup_then_v27 -- --nocapture

        use micro_blossom_nostd::util::*;

        let graph = MicroBlossomSingle::phenomenological_rotated_d3_archive_v24_v27_then_v27();
        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let warmup_rounds = num_layers.saturating_sub(1);
        let top_verts: Vec<usize> = graph.layer_fusion.as_ref().unwrap().layers.last().unwrap().clone();
        let layer_0_vertices: std::collections::HashSet<u32> = graph.layer_fusion.as_ref().unwrap().layers[0]
            .iter()
            .map(|vertex| *vertex as u32)
            .collect();
        let (mut dual, mut primal, graph, top_layer) = make_streaming_env_from_graph(graph, 8);

        // Fuse all layers at init so layer vertices are non-virtual and can grow.
        // Without this, v19/v11/v3 stay virtual (speed=0) and archived conflicts
        // on fusion edges never trigger (live_grown stays 0).
        for layer_id in 0..num_layers {
            dual.fuse_layer(layer_id as CompactLayerNum);
            primal.fuse_layer(&mut *dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
        }

        assert!(
            graph.vertex_num > 27,
            "graph must include v27 (vertex_num={})",
            graph.vertex_num
        );

        println!("\n{}", "=".repeat(72));
        println!("  boundary7_v24_v27_warmup_then_v27 — graph setup");
        println!("{}", "=".repeat(72));
        println!(
            "  graph: phenomenological_rotated_d3_archive_v24_v27_then_v27 (vertex_num={}, archive_depth=8)",
            graph.vertex_num
        );
        println!(
            "  weights: base real-real = 20 | base boundary = 100 | cheap path: (27,19),(19,11),(11,3)=1 | (24,27)=120 | v0-boundary = 250 | v24-column edges to layer 0 = 200 | v24-v26 escape = 400 | direct v27 boundary edges = 300 | side exits off the 27-19-11-3 column = 400 | lateral real-real exits off that column = 400 | v27-v28 detour = 400 | (0,3)=200"
        );
        println!("  fusion: num_layers={num_layers}, top_layer id={top_layer}, top vertices {top_verts:?}");
        println!(
            "  plan: R0 syndromes v24+v27 (nodes 0,1) → {warmup_rounds}× empty fuse/solve/archive → final v27 (node 2)"
        );
        println!("{}\n", "=".repeat(72));

        println!("{}", "=".repeat(72));
        println!("  ROUND R0 — load 2 defects, fuse top layer, decode, archive");
        println!("{}", "=".repeat(72));
        println!("  syndromes: vertices [24, 27]  (decoder nodes 0 and 1)");
        println!("  action: dual.add_defect ×2 → fuse_layer({top_layer}) → primal.fuse_layer → find_obstacle/resolve loop → archive_elastic_slice");
        streaming_round_verbose(&mut dual, &mut primal, &[24, 27], 0, top_layer, "R0", true);
        let r0_matchings = collect_outer_matchings(&primal);
        assert_peer_pair(&r0_matchings, 0, 1);
        print_outer_matching_line(&primal, "after R0");
        println!();

        println!("{}", "=".repeat(72));
        println!("  WARMUP — {warmup_rounds} empty measurement rounds (elastic archive / chain shift)");
        println!("{}", "=".repeat(72));
        for i in 0..warmup_rounds {
            println!(
                "\n  --- warmup step {}/{}: no new defects (node_offset=2 for next ids) ---",
                i + 1,
                warmup_rounds
            );
            println!("  action: fuse_layer → (no obstacles expected) → archive_elastic_slice");
            streaming_round_verbose(
                &mut dual,
                &mut primal,
                &[],
                2,
                top_layer,
                &format!("warmup_{i}"),
                true,
            );
            print_outer_matching_line(&primal, &format!("after warmup_{i}"));
        }
        println!();

        println!("{}", "=".repeat(72));
        println!("  ROUND R_final — single new defect on live top v27");
        println!("{}", "=".repeat(72));
        println!("  syndrome: vertex [27]  (decoder node 2)");
        println!("  action: manual fuse/solve to capture archived conflicts before the final archive");
        print_dual_vertex_columns(&dual, "pre_R_final", &[0, 3, 16, 19]);
        dual.add_defect(ni!(27), ni!(2));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());
        print_dual_vertex_columns(&dual, "R_final_after_fuse", &[0, 3, 16, 19]);

        let mut final_iterations = 0;
        let mut saw_archived_or_cross_layer_conflict = false;
        let (mut obstacle, grown) = dual.find_obstacle();
        println!("  [R_final] initial obstacle: {} (grown={grown})", format_obstacle(&obstacle));
        print_dual_vertex_columns(&dual, "R_final_after_find_obstacle", &[0, 3, 16, 19]);
        while !obstacle.is_none() {
            if let CompactObstacle::Conflict { vertex_1, vertex_2, .. } = obstacle {
                let v1 = vertex_1.get();
                let v2 = vertex_2.get();
                if layer_0_vertices.contains(&v1) || layer_0_vertices.contains(&v2) {
                    saw_archived_or_cross_layer_conflict = true;
                }
            }
            println!("  [R_final] iter {final_iterations}: resolving {}", format_obstacle(&obstacle));
            primal.resolve(&mut *dual, obstacle);
            let (next_obstacle, next_grown) = dual.find_obstacle();
            obstacle = next_obstacle;
            println!("  [R_final] iter {final_iterations}: after resolve → {} (grown={next_grown})", format_obstacle(&obstacle));
            final_iterations += 1;
            assert!(final_iterations < 2000, "R_final: solve loop stuck after {final_iterations} iterations");
        }
        let final_matchings = collect_outer_matchings(&primal);
        println!("  [R_final] final matchings before archive: {final_matchings:?}");
        print_dual_vertex_columns(&dual, "R_final_after_solve", &[0, 3, 16, 19]);
        assert!(
            saw_archived_or_cross_layer_conflict,
            "expected final round to touch archived / layer-0 state before quiescing; final_matchings={final_matchings:?}"
        );
        print_outer_matching_line(&primal, "after R_final solve");
        archive_streaming_slice(&mut dual);

        println!("\n{}", "=".repeat(72));
        println!("  FINAL primal state (all printed outer nodes)");
        println!("{}", "=".repeat(72));
        primal.nodes.iterate_intermediate_matching(|node, target, _link| {
            let grow = primal.nodes.get_node(node).grow_state;
            println!("  node {} (grow={:?}) → {:?}", node.get(), grow, target);
        });

        println!("\n{}", "=".repeat(72));
        println!("  dual_module_looper_boundary7_v24_v27_warmup_then_v27 — PASS");
        println!("{}\n", "=".repeat(72));
    }

    /// Reference optimum for [`dual_module_looper_boundary7_v24_v27_warmup_then_v27`](Self::dual_module_looper_boundary7_v24_v27_warmup_then_v27):
    /// same graph and streaming steps using the **combinatorial** dual (no Scala). The looper
    /// test asserts the **same** final matching pattern so any Scala / comb mismatch is caught in CI.
    #[test]
    fn dual_module_looper_boundary7_v24_v27_warmup_then_v27_comb_reference() {
        use micro_blossom_nostd::util::*;

        let graph = MicroBlossomSingle::phenomenological_rotated_d3_archive_v24_v27_then_v27();
        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let warmup_rounds = num_layers.saturating_sub(1);

        let (mut dual, mut primal, graph, top_layer) = make_streaming_comb_from_graph(graph, 8);

        // Fuse all layers at init (same as hardware test)
        for layer_id in 0..num_layers {
            dual.fuse_layer(layer_id as CompactLayerNum);
            primal.fuse_layer(&mut dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
        }

        streaming_round_verbose(&mut dual, &mut primal, &[24, 27], 0, top_layer, "R0", false);
        let r0_matchings = collect_outer_matchings(&primal);
        assert_peer_pair(&r0_matchings, 0, 1);
        for i in 0..warmup_rounds {
            streaming_round_verbose(
                &mut dual,
                &mut primal,
                &[],
                2,
                top_layer,
                &format!("warmup_{i}"),
                false,
            );
        }

        dual.add_defect(ni!(27), ni!(2));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut dual, CompactLayerId::new(top_layer).unwrap());

        let mut final_iterations = 0;
        let (mut obstacle, _) = dual.find_obstacle();
        while !obstacle.is_none() {
            primal.resolve(&mut dual, obstacle);
            (obstacle, _) = dual.find_obstacle();
            final_iterations += 1;
            assert!(
                final_iterations < 2000,
                "comb reference: solve loop stuck after {final_iterations} iterations"
            );
        }

        let final_matchings = collect_outer_matchings(&primal);
        let all_nodes_matched = final_matchings.len() >= 2;
        assert!(all_nodes_matched, "comb reference: not all nodes matched: {final_matchings:?}");
    }

    /// Two live defects vs one archived pair: both v24 and v27 get new defects after warmup.
    /// This forces the primal to resolve archived conflicts involving all 4 nodes (0,1 archived + 2,3 live).
    /// The archived pair n0↔n1 interacts with both live defects, testing richer alternating tree formation.
    #[test]
    fn dual_module_looper_boundary7_v24v27_warmup_then_v24v27() {
        use micro_blossom_nostd::util::*;

        let graph = MicroBlossomSingle::phenomenological_rotated_d3_archive_v24_v27_then_v27();
        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let warmup_rounds = num_layers.saturating_sub(1);
        let layer_0_vertices: std::collections::HashSet<u32> = graph.layer_fusion.as_ref().unwrap().layers[0]
            .iter()
            .map(|vertex| *vertex as u32)
            .collect();
        let (mut dual, mut primal, graph, top_layer) = make_streaming_env_from_graph(graph, 8);

        // Fuse all layers at init
        for layer_id in 0..num_layers {
            dual.fuse_layer(layer_id as CompactLayerNum);
            primal.fuse_layer(&mut *dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
        }

        println!("\n  === R0: defects v24+v27 (nodes 0,1) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[24, 27], 0, top_layer, "R0", true);
        let r0_matchings = collect_outer_matchings(&primal);
        assert_peer_pair(&r0_matchings, 0, 1);

        println!("\n  === WARMUP ({warmup_rounds} rounds) ===");
        for i in 0..warmup_rounds {
            streaming_round_verbose(&mut dual, &mut primal, &[], 2, top_layer, &format!("warmup_{i}"), false);
        }

        println!("\n  === R_final: defects v24+v27 (nodes 2,3) — both interact with archived pair ===");
        dual.add_defect(ni!(24), ni!(2));
        dual.add_defect(ni!(27), ni!(3));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        let mut final_iterations = 0;
        let mut saw_archived_conflict = false;
        let (mut obstacle, grown) = dual.find_obstacle();
        println!("  [R_final] initial obstacle: {} (grown={grown})", format_obstacle(&obstacle));
        while !obstacle.is_none() {
            if let CompactObstacle::Conflict { vertex_1, vertex_2, .. } = obstacle {
                let v1 = vertex_1.get();
                let v2 = vertex_2.get();
                if layer_0_vertices.contains(&v1) || layer_0_vertices.contains(&v2) {
                    saw_archived_conflict = true;
                }
            }
            println!("  [R_final] iter {final_iterations}: resolving {}", format_obstacle(&obstacle));
            primal.resolve(&mut *dual, obstacle);
            let (next_obstacle, next_grown) = dual.find_obstacle();
            obstacle = next_obstacle;
            println!("  [R_final] iter {final_iterations}: after resolve → {} (grown={next_grown})", format_obstacle(&obstacle));
            final_iterations += 1;
            assert!(final_iterations < 2000, "R_final: solve loop stuck after {final_iterations} iterations");
        }
        let final_matchings = collect_outer_matchings(&primal);
        println!("  [R_final] final matchings: {final_matchings:?}");
        assert!(
            saw_archived_conflict,
            "expected archived / cross-layer conflict; final_matchings={final_matchings:?}"
        );
        // All 4 nodes (0,1,2,3) must be matched
        assert!(final_matchings.len() >= 2, "not all nodes matched: {final_matchings:?}");
        println!("\n  === dual_module_looper_boundary7_v24v27_warmup_then_v24v27 — PASS ===");
    }

    /// Three rounds of defects at v24+v27, each separated by `numLayers-1` empty flush rounds
    /// so the live state is clean before the next defect round. All 3 pairs archive identically
    /// (grown=60 each). The final defect at v27 triggers a chain of archived conflicts through
    /// all 3 entries, producing an alternating tree: n0→boundary, n1↔n6, n2↔n5, n3↔n4.
    #[test]
    fn dual_module_looper_boundary7_multi_round_alternating_tree() {
        use micro_blossom_nostd::util::*;

        let graph = MicroBlossomSingle::phenomenological_rotated_d3_archive_v24_v27_then_v27();
        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let flush_rounds = num_layers.saturating_sub(1); // 3 empty rounds to flush shift chain
        let layer_0_vertices: std::collections::HashSet<u32> = graph.layer_fusion.as_ref().unwrap().layers[0]
            .iter()
            .map(|vertex| *vertex as u32)
            .collect();
        // 3 defect rounds × (1 defect + 3 flush) = 12 archives before R_final warmup.
        // Plus 3 warmup archives = 15 total. But only 3 carry real data; the rest are empty.
        // archiveDepth=16 is enough to hold everything.
        let archive_depth = 16;
        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env_from_graph(graph, archive_depth);

        // Fuse all layers at init
        for layer_id in 0..num_layers {
            dual.fuse_layer(layer_id as CompactLayerNum);
            primal.fuse_layer(&mut *dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
        }

        // --- 3 defect rounds, each followed by flush_rounds empty rounds ---
        let defect_rounds = 3;
        let mut node_offset = 0usize;
        for round in 0..defect_rounds {
            println!("\n  === R{round}: defects v24+v27 (nodes {node_offset},{}) ===", node_offset + 1);
            streaming_round_verbose(
                &mut dual, &mut primal, &[24, 27], node_offset, top_layer,
                &format!("R{round}"), true,
            );
            let matchings = collect_outer_matchings(&primal);
            assert_peer_pair(&matchings, node_offset as u32, (node_offset + 1) as u32);
            node_offset += 2;

            // Flush: empty rounds to shift data to L0 and clear live state
            for f in 0..flush_rounds {
                streaming_round_verbose(
                    &mut dual, &mut primal, &[], node_offset, top_layer,
                    &format!("R{round}_flush_{f}"), false,
                );
            }
        }

        // --- final warmup: ensure the last defect round's data reaches L0 ---
        // (The flush rounds above already handle this for earlier rounds.
        //  The last defect round's flush rounds bring its data to L0.)

        // --- Dump all archived entries for v0 and v3 before R_final ---
        print_dual_vertex_columns(&dual, "pre_R_final", &[0, 3]);

        // --- R_final: single defect at v27 triggers chain of archived conflicts ---
        let final_node = node_offset; // = 6
        println!("\n  === R_final: defect v27 (node {final_node}) ===");
        dual.add_defect(ni!(27), ni!(final_node));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());

        let mut final_iterations = 0;
        let mut archived_conflict_count = 0;
        let (mut obstacle, grown) = dual.find_obstacle();
        println!("  [R_final] initial obstacle: {} (grown={grown})", format_obstacle(&obstacle));
        while !obstacle.is_none() {
            if let CompactObstacle::Conflict { vertex_1, vertex_2, .. } = obstacle {
                let v1 = vertex_1.get();
                let v2 = vertex_2.get();
                if layer_0_vertices.contains(&v1) || layer_0_vertices.contains(&v2) {
                    archived_conflict_count += 1;
                }
            }
            println!("  [R_final] iter {final_iterations}: resolving {}", format_obstacle(&obstacle));
            primal.resolve(&mut *dual, obstacle);
            let (next_obstacle, next_grown) = dual.find_obstacle();
            obstacle = next_obstacle;
            println!("  [R_final] iter {final_iterations}: after resolve → {} (grown={next_grown})", format_obstacle(&obstacle));
            final_iterations += 1;
            assert!(final_iterations < 2000, "R_final: solve loop stuck after {final_iterations} iterations");
        }

        let final_matchings = collect_outer_matchings(&primal);
        println!("  [R_final] final matchings: {final_matchings:?}");
        println!("  archived conflicts seen: {archived_conflict_count}");
        assert!(
            archived_conflict_count >= 1,
            "expected at least 1 archived conflict; saw {archived_conflict_count}; matchings={final_matchings:?}"
        );
        assert!(!final_matchings.is_empty(), "no matchings produced");
        println!("\n  === dual_module_looper_boundary7_multi_round_alternating_tree — PASS ===");
    }

    /// Shortest hop count between two vertices (unweighted BFS on `weighted_edges`).
    fn graph_hop_distance(graph: &crate::resources::MicroBlossomSingle, start: usize, goal: usize) -> Option<usize> {
        use std::collections::VecDeque;
        if start == goal {
            return Some(0);
        }
        let n = graph.vertex_num;
        let mut dist: Vec<Option<usize>> = vec![None; n];
        let mut q = VecDeque::new();
        dist[start] = Some(0);
        q.push_back(start);
        while let Some(u) = q.pop_front() {
            let du = dist[u].unwrap();
            for e in &graph.weighted_edges {
                let v = if e.l == u {
                    e.r
                } else if e.r == u {
                    e.l
                } else {
                    continue;
                };
                if dist[v].is_none() {
                    dist[v] = Some(du + 1);
                    if v == goal {
                        return Some(du + 1);
                    }
                    q.push_back(v);
                }
            }
        }
        None
    }

    /// Pick two **distinct** interior top vertices with **maximum** hop distance on the decoder
    /// graph. Close pairs make a cheap spatial matching `0↔1` and `2↔3`; far pairs raise the cost
    /// of pairing old-with-old so the optimum uses **time-like** peers `0↔2` and `1↔3` instead.
    fn interior_top_pair_max_hops(
        graph: &crate::resources::MicroBlossomSingle,
        interior_top: &[usize],
    ) -> (usize, usize) {
        assert!(interior_top.len() >= 2);
        let mut best = (interior_top[0], interior_top[1]);
        let mut best_d = 0usize;
        for (i, &va) in interior_top.iter().enumerate() {
            for &vb in interior_top.iter().skip(i + 1) {
                if let Some(d) = graph_hop_distance(graph, va, vb) {
                    if d > best_d {
                        best_d = d;
                        best = (va, vb);
                    }
                }
            }
        }
        best
    }

    /// Like `dual_module_looper_archive_dependent_matching`, but **two** interior top defects in
    /// round 0 (nodes 0 and 1 on **widely separated** columns), both archived **without** solving.
    /// After one empty warmup round (`noisy_measurements=1` → 2 layers), round 2 places **two**
    /// new defects on the **same** top vertices (nodes 2 and 3). Asserts time-like peers `0↔2`
    /// and `1↔3`. Columns are chosen by maximum graph hop distance among interior top sites so
    /// spatial `0↔1` / `2↔3` is not the cheaper MWPM outcome.
    #[test]
    fn dual_module_looper_archive_multi_defect_column_pairing() {
        // WITH_WAVEFORM=1 KEEP_RTL_FOLDER=1 cargo test dual_module_looper_archive_multi_defect_column_pairing -- --nocapture
        use micro_blossom_nostd::util::*;

        let (mut dual, mut primal, graph, top_layer) = make_streaming_env_custom(5, 1, 8);
        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;
        let warmup_rounds = num_layers.saturating_sub(1);
        let top_verts: Vec<usize> = graph.layer_fusion.as_ref().unwrap().layers
            .last().unwrap().iter().map(|v| *v as usize).collect();
        let virt: std::collections::HashSet<usize> = graph.virtual_vertices.iter().map(|v| *v as usize).collect();

        let is_interior = |v: usize| -> bool {
            !graph.weighted_edges.iter().any(|e| {
                (e.l == v && virt.contains(&e.r)) || (e.r == v && virt.contains(&e.l))
            })
        };
        let interior_top: Vec<usize> = top_verts.iter().copied().filter(|&v| is_interior(v)).collect();
        assert!(
            interior_top.len() >= 2,
            "need two interior top vertices (got {})",
            interior_top.len()
        );
        let (v_a, v_b) = interior_top_pair_max_hops(&graph, &interior_top);

        // Round 0: two defects, fuse only — no solve — archive.
        dual.add_defect(ni!(v_a), ni!(0));
        dual.add_defect(ni!(v_b), ni!(1));
        dual.fuse_layer(top_layer);
        primal.fuse_layer(&mut *dual, CompactLayerId::new(top_layer).unwrap());
        archive_streaming_slice(&mut *dual);

        for i in 0..warmup_rounds {
            streaming_round_verbose(
                &mut dual,
                &mut primal,
                &[],
                0,
                top_layer,
                &format!("R_empty_{i}"),
                false,
            );
        }

        // Same two top sites, nodes 2 and 3.
        streaming_round_verbose(
            &mut dual,
            &mut primal,
            &[v_a, v_b],
            2,
            top_layer,
            "R_final",
            false,
        );

        let mut matchings = vec![];
        primal.nodes.iterate_intermediate_matching(|node, target, _link| {
            matchings.push((node.get(), target));
        });
        let peer_to = |from: u32, to: u32| {
            matchings.iter().any(|(n, t)| {
                *n == from && matches!(t, CompactMatchTarget::Peer(p) if p.get() == to)
            })
        };
        assert!(
            peer_to(0, 2),
            "expected node 0 peer-matched to 2 (matchings={matchings:?})"
        );
        assert!(
            peer_to(1, 3),
            "expected node 1 peer-matched to 3 (matchings={matchings:?})"
        );
    }

    /// Asymmetric archive smoke test (d=3 phenomenological, four layers, `archive_depth=8`).
    ///
    /// **Geometry:** Round 0 seeds only column **v24** (maps to layer-0 **v0**). After warmup shifts,
    /// round 3 adds a defect on **v27** (maps to layer-0 **v3**) — a **different** spatial column, so
    /// archived elastic data exists under one column but not the other. Round 4 loads both sites
    /// together so the dual must handle **mixed** archive presence on the fused graph.
    ///
    /// Each round uses `streaming_round_verbose` → `archive_streaming_slice` after a quiescent solve.
    ///
    /// The final primal may show a **blossom** outer index (e.g. node 32) after resolving the
    /// pair — that is normal MWPM, not a failure.
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
        println!("\n=== Round 3: defect on v27 (node=1) ===");
        streaming_round_verbose(&mut dual, &mut primal, &[27], 1, top_layer, "R3", true);
        print_state(&dual, &primal, "R3");

        // Round 4: defects on both v24 and v27
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

    /// Verify that periodic reset after archive BRAM fills works correctly.
    ///
    /// Uses `archive_depth=4` so the archive fills after 4 rounds. Runs 20 rounds total
    /// with a reset every 4 rounds (mimicking `benchmark_decoding.rs` streaming logic).
    /// Verifies the decoder doesn't hang and produces valid matchings after each reset.
    ///
    /// ```sh
    /// cargo test dual_module_looper_streaming_archive_reset -- --nocapture
    /// ```
    #[test]
    fn dual_module_looper_streaming_archive_reset() {
        use micro_blossom_nostd::util::*;

        let graph = MicroBlossomSingle::phenomenological_rotated_d3_archive_v24_v27_then_v27();
        let archive_depth = 4;
        let total_rounds = 20;
        let (mut dual, mut primal, _graph, top_layer) = make_streaming_env_from_graph(graph, archive_depth);

        // Fuse all layers at init
        let num_layers = _graph.layer_fusion.as_ref().unwrap().num_layers;
        for layer_id in 0..num_layers {
            dual.fuse_layer(layer_id as CompactLayerNum);
            primal.fuse_layer(&mut *dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
        }

        let mut node_offset = 0usize;
        let mut rounds_since_reset = 0usize;
        let mut reset_count = 0usize;

        for round in 0..total_rounds {
            // Alternate between defect rounds (v24+v27) and empty rounds
            let defects: &[usize] = if round % 2 == 0 { &[24, 27] } else { &[] };

            streaming_round(
                &mut dual, &mut primal, defects, node_offset, top_layer,
                &format!("R{round}"),
            );
            node_offset += defects.len();
            rounds_since_reset += 1;

            // Reset when archive is full, just like benchmark_decoding.rs
            if rounds_since_reset >= archive_depth {
                println!("  [round {round}] resetting (archive full after {rounds_since_reset} rounds)");
                primal.reset();
                dual.reset();
                node_offset = 0;
                rounds_since_reset = 0;
                reset_count += 1;

                // Re-fuse layers after reset (same as firmware cold start)
                for layer_id in 0..num_layers {
                    dual.fuse_layer(layer_id as CompactLayerNum);
                    primal.fuse_layer(&mut *dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
                }
            }
        }

        assert!(reset_count >= 2, "expected at least 2 resets; got {reset_count}");
        println!("\n  === dual_module_looper_streaming_archive_reset — PASS ===");
        println!("  {total_rounds} rounds, archive_depth={archive_depth}, {reset_count} resets");
    }

    /// Stress test: streaming decode with random defects at varying error rates,
    /// mimicking the `run.py` benchmark sweep. Uses seeded RNG to select random
    /// top-layer vertices as defects each round, with periodic resets when the
    /// archive fills (same logic as `benchmark_decoding.rs`).
    ///
    /// This catches solver hangs that only manifest with specific defect patterns
    /// at higher error rates (e.g. the p≈2.5e-4 hang seen on hardware).
    ///
    /// ```sh
    /// cargo test dual_module_looper_streaming_random_defects -- --nocapture
    /// ```
    #[test]
    fn dual_module_looper_streaming_random_defects() {
        use micro_blossom_nostd::util::*;
        use rand_xoshiro::rand_core::{RngCore, SeedableRng};
        use rand_xoshiro::Xoshiro256StarStar;

        let archive_depth = 8;
        let rounds_per_p = 50;
        // Test multiple error rates including the problematic range
        let p_values = [0.0001, 0.00025, 0.001, 0.005, 0.01];

        let graph_path = format!(
            "{}/../../../resources/graphs/example_phenomenological_rotated_d3.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let json_str = std::fs::read_to_string(&graph_path)
            .unwrap_or_else(|e| panic!("read graph {graph_path}: {e}"));
        let graph: MicroBlossomSingle = serde_json::from_str(&json_str).expect("parse graph");
        let num_layers = graph.layer_fusion.as_ref().unwrap().num_layers;

        // Collect top-layer vertices
        let top_layer_id = num_layers - 1;
        let top_vertices: Vec<usize> = graph.layer_fusion.as_ref().unwrap().layers[top_layer_id]
            .iter()
            .map(|&v| v)
            .collect();
        assert!(!top_vertices.is_empty(), "no top-layer vertices found");

        for &p in &p_values {
            println!("\n  === p={p:.1e}, {rounds_per_p} rounds, archive_depth={archive_depth} ===");

            let (mut dual, mut primal, _graph, top_layer) =
                make_streaming_env_from_graph(graph.clone(), archive_depth);

            // Fuse all layers at init
            for layer_id in 0..num_layers {
                dual.fuse_layer(layer_id as CompactLayerNum);
                primal.fuse_layer(&mut *dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
            }

            let mut rng = Xoshiro256StarStar::seed_from_u64((p * 1e9) as u64);
            let mut node_offset = 0usize;
            let mut rounds_since_reset = 0usize;
            let mut reset_count = 0usize;
            let mut total_defects = 0usize;

            for round in 0..rounds_per_p {
                // Generate random defects: each top-layer vertex is a defect with probability p
                // Scale p up since d=3 graph has few vertices and low p would rarely produce defects
                let effective_p = f64::min(p * 100.0, 0.5); // scale for test visibility
                let defects: Vec<usize> = top_vertices.iter()
                    .filter(|_| (rng.next_u32() as f64 / u32::MAX as f64) < effective_p)
                    .copied()
                    .collect();

                streaming_round(
                    &mut dual, &mut primal, &defects, node_offset, top_layer,
                    &format!("p{p:.0e}_R{round}"),
                );
                node_offset += defects.len();
                total_defects += defects.len();
                rounds_since_reset += 1;

                // Reset when archive is full, same as benchmark_decoding.rs
                if rounds_since_reset >= archive_depth
                    || node_offset > MAX_NODE_NUM - 256
                {
                    primal.reset();
                    dual.reset();
                    node_offset = 0;
                    rounds_since_reset = 0;
                    reset_count += 1;

                    for layer_id in 0..num_layers {
                        dual.fuse_layer(layer_id as CompactLayerNum);
                        primal.fuse_layer(&mut *dual, CompactLayerId::new(layer_id as CompactLayerNum).unwrap());
                    }
                }
            }

            println!(
                "  p={p:.1e}: {rounds_per_p} rounds, {total_defects} total defects, {reset_count} resets — PASS"
            );
        }

        println!("\n  === dual_module_looper_streaming_random_defects — ALL PASS ===");
    }
}
