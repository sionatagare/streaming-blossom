package microblossom.modules

import spinal.core._
import spinal.lib._
import microblossom._
import microblossom.types._
import microblossom.stage._
import microblossom.combinatorial._
import microblossom.util.Vivado
import org.scalatest.funsuite.AnyFunSuite

object Edge {
  def getStages(
      config: DualConfig
  ): Stages[
    StageOffloadEdge,
    StageOffloadEdge2,
    StageOffloadEdge3,
    StageOffloadEdge4,
    StageExecuteEdge,
    StageExecuteEdge2,
    StageExecuteEdge3,
    StageUpdateEdge,
    StageUpdateEdge2,
    StageUpdateEdge3
  ] = {
    Stages(
      offload = () => StageOffloadEdge(config),
      offload2 = () => StageOffloadEdge2(config),
      offload3 = () => StageOffloadEdge3(config),
      offload4 = () => StageOffloadEdge4(config),
      execute = () => StageExecuteEdge(config),
      execute2 = () => StageExecuteEdge2(config),
      execute3 = () => StageExecuteEdge3(config),
      update = () => StageUpdateEdge(config),
      update2 = () => StageUpdateEdge2(config),
      update3 = () => StageUpdateEdge3(config)
    )
  }
}

case class Edge(config: DualConfig, edgeIndex: Int) extends Component {
  val (leftVertex, rightVertex) = config.incidentVerticesOf(edgeIndex)
  val leftGrownBits = config.grownBitsOf(leftVertex)
  val rightGrownBits = config.grownBitsOf(rightVertex)

  val io = new Bundle {
    val message = in(BroadcastMessage(config))
    // interaction I/O
    val stageOutputs = out(Edge.getStages(config).getStageOutput)
    val leftVertexInput = in(Vertex.getStages(config, leftVertex).getStageOutput)
    val rightVertexInput = in(Vertex.getStages(config, rightVertex).getStageOutput)
    // Layer-0 counterpart vertex stage outputs (for archived state at pipeline stages)
    val leftL0Vertex = config.layer0CounterpartOf(leftVertex)
    val rightL0Vertex = config.layer0CounterpartOf(rightVertex)
    val leftL0VertexInput = in(Vertex.getStages(config, leftL0Vertex).getStageOutput)
    val rightL0VertexInput = in(Vertex.getStages(config, rightL0Vertex).getStageOutput)
    val edgeScanActive = in(Bool())
    val edgeScanIndex = in UInt (config.archiveAddressBits bits)
    // final outputs
    val maxGrowable = out(ConvergecastMaxGrowable(config.weightBits))
    val conflict = out(ConvergecastConflict(config.vertexBits))
  }

  val stages = Edge.getStages(config)
  stages.connectStageOutput(io.stageOutputs)

  // fetch
  var ram: Mem[EdgeState] = null
  var register = (!config.hardCodeWeights) generate Reg(EdgeState(config.weightBits, config.hardCodeWeights))
  if (!config.hardCodeWeights) {
    register.init(EdgeState.resetValue(config, edgeIndex))
  }
  var fetchState = EdgeState(config.weightBits)
  var message = BroadcastMessage(config)
  if (config.contextBits > 0) {
    // fetch stage, delay the instruction
    if (config.hardCodeWeights) {
      fetchState := EdgeState.resetValue(config, edgeIndex)
    } else {
      ram = Mem(EdgeState(config.weightBits), config.contextDepth)
      ram.setTechnology(ramBlock)
      fetchState := ram.readSync(
        address = io.message.contextId,
        enable = io.message.valid
      )
    }
    message := RegNext(io.message)
  } else {
    if (config.hardCodeWeights) {
      fetchState := EdgeState.resetValue(config, edgeIndex)
    } else {
      fetchState := register
    }
    message := io.message
  }

  stages.offloadSet.state := fetchState
  stages.offloadSet.compact.connect(message)

  stages.offloadSet2.connect(stages.offloadGet) // weight might be changed later
  val hasLayerFusion = config.edgeConditionedVertex.contains(edgeIndex)
  val conditionedVertexIsVirtual = if (hasLayerFusion) {
    val conditionedVertex = config.edgeConditionedVertex(edgeIndex)
    if (conditionedVertex == leftVertex) {
      io.leftVertexInput.offloadGet.state.isVirtual
    } else if (conditionedVertex == rightVertex) {
      io.rightVertexInput.offloadGet.state.isVirtual
    } else {
      throw new Exception("cannot find the conditioned vertex")
    }
  } else {
    False
  }
  // weight might be changed because of layer fusion
  val offload2Weight = UInt(config.weightBits bits)
  offload2Weight := stages.offloadGet.state.weight
  if (hasLayerFusion) {
    when(conditionedVertexIsVirtual) {
      offload2Weight := (stages.offloadGet.state.weight |>> 2) |<< 1
      stages.offloadSet2.state.weight := offload2Weight
    }
  }

  // Archived state comes from layer-0 counterpart vertex's archivedState in pipeline stages
  val conditionedVertexIsVirtualArch = if (hasLayerFusion) {
    val conditionedVertex = config.edgeConditionedVertex(edgeIndex)
    val condL0 = config.layer0CounterpartOf(conditionedVertex)
    if (config.vertexHasElasticLayers(condL0)) {
      if (conditionedVertex == leftVertex) io.leftL0VertexInput.offloadGet.archivedState.isVirtual
      else io.rightL0VertexInput.offloadGet.archivedState.isVirtual
    } else {
      io.leftVertexInput.offloadGet.state.isVirtual // fallback for non-elastic
    }
  } else {
    False
  }

  val leftGrownOffloadLayers =
    if (config.vertexHasElasticLayers(io.leftL0Vertex)) io.leftL0VertexInput.offloadGet.archivedState.grown
    else io.leftVertexInput.offloadGet.state.grown
  val rightGrownOffloadLayers =
    if (config.vertexHasElasticLayers(io.rightL0Vertex)) io.rightL0VertexInput.offloadGet.archivedState.grown
    else io.rightVertexInput.offloadGet.state.grown

  val offload2WeightArch = UInt(config.weightBits bits)
  offload2WeightArch := stages.offloadGet.state.weight
  if (hasLayerFusion) {
    when(conditionedVertexIsVirtualArch) {
      offload2WeightArch := (stages.offloadGet.state.weight |>> 2) |<< 1
    }
  }

  val offload2Area = new Area {
    val edgeIsTight = EdgeIsTight(
      leftGrownBits = leftGrownBits,
      rightGrownBits = rightGrownBits,
      weightBits = config.weightBits
    )
    edgeIsTight.io.leftGrown := io.leftVertexInput.offloadGet.state.grown
    edgeIsTight.io.rightGrown := io.rightVertexInput.offloadGet.state.grown
    edgeIsTight.io.weight := offload2Weight
    stages.offloadSet2.isTight := edgeIsTight.io.isTight
    if (hasLayerFusion) {
      stages.offloadSet2.isTightExFusion := edgeIsTight.io.isTight && !conditionedVertexIsVirtual
    } else {
      stages.offloadSet2.isTightExFusion := edgeIsTight.io.isTight
    }

    val edgeIsTightLayers = EdgeIsTight(
      leftGrownBits = leftGrownBits,
      rightGrownBits = rightGrownBits,
      weightBits = config.weightBits
    )
    edgeIsTightLayers.io.leftGrown := leftGrownOffloadLayers
    edgeIsTightLayers.io.rightGrown := rightGrownOffloadLayers
    edgeIsTightLayers.io.weight := offload2WeightArch
    stages.offloadSet2.isTightVsElasticLayers := edgeIsTightLayers.io.isTight
    if (hasLayerFusion) {
      stages.offloadSet2.isTightExFusionVsElasticLayers :=
        edgeIsTightLayers.io.isTight && !conditionedVertexIsVirtualArch
    } else {
      stages.offloadSet2.isTightExFusionVsElasticLayers := edgeIsTightLayers.io.isTight
    }
  }

  stages.offloadSet3.connect(stages.offloadGet2)
  stages.offloadSet3.isTight :=
    stages.offloadGet2.isTight || stages.offloadGet2.isTightVsElasticLayers

  stages.offloadSet4.connect(stages.offloadGet3)

  // TODO: dynamically update edge weights
  // Mux(message.isReset, EdgeState.resetValue(config, edgeIndex), fetchState)
  stages.executeSet.connect(stages.offloadGet4)

  stages.executeSet2.connect(stages.executeGet)

  stages.executeSet3.connect(stages.executeGet2)

  val conditionedVertexIsVirtualExecuteArch = if (hasLayerFusion) {
    val conditionedVertex = config.edgeConditionedVertex(edgeIndex)
    val condL0 = config.layer0CounterpartOf(conditionedVertex)
    if (config.vertexHasElasticLayers(condL0)) {
      if (conditionedVertex == leftVertex) io.leftL0VertexInput.executeGet2.archivedState.isVirtual
      else io.rightL0VertexInput.executeGet2.archivedState.isVirtual
    } else {
      io.leftVertexInput.executeGet2.state.isVirtual
    }
  } else {
    False
  }

  val leftGrownExecuteLayers =
    if (config.vertexHasElasticLayers(io.leftL0Vertex)) io.leftL0VertexInput.executeGet2.archivedState.grown
    else io.leftVertexInput.executeGet2.state.grown
  val rightGrownExecuteLayers =
    if (config.vertexHasElasticLayers(io.rightL0Vertex)) io.rightL0VertexInput.executeGet2.archivedState.grown
    else io.rightVertexInput.executeGet2.state.grown

  val executeWeightArch = UInt(config.weightBits bits)
  executeWeightArch := stages.executeGet2.state.weight
  if (hasLayerFusion) {
    when(conditionedVertexIsVirtualExecuteArch) {
      executeWeightArch := (stages.executeGet2.state.weight |>> 2) |<< 1
    }
  }

  val execute3Area = new Area {
    val edgeIsTight = EdgeIsTight(
      leftGrownBits = leftGrownBits,
      rightGrownBits = rightGrownBits,
      weightBits = config.weightBits
    )
    edgeIsTight.io.leftGrown := io.leftVertexInput.executeGet2.state.grown
    edgeIsTight.io.rightGrown := io.rightVertexInput.executeGet2.state.grown
    edgeIsTight.io.weight := stages.executeGet2.state.weight
    stages.executeSet3.isTight := edgeIsTight.io.isTight

    val edgeIsTightLayers = EdgeIsTight(
      leftGrownBits = leftGrownBits,
      rightGrownBits = rightGrownBits,
      weightBits = config.weightBits
    )
    edgeIsTightLayers.io.leftGrown := leftGrownExecuteLayers
    edgeIsTightLayers.io.rightGrown := rightGrownExecuteLayers
    edgeIsTightLayers.io.weight := executeWeightArch
    stages.executeSet3.isTightVsElasticLayers := edgeIsTightLayers.io.isTight
  }

  stages.updateSet.connect(stages.executeGet3)

  val leftGrownUpdateLayers =
    if (config.vertexHasElasticLayers(io.leftL0Vertex)) io.leftL0VertexInput.executeGet3.archivedState.grown
    else io.leftVertexInput.executeGet3.state.grown
  val rightGrownUpdateLayers =
    if (config.vertexHasElasticLayers(io.rightL0Vertex)) io.rightL0VertexInput.executeGet3.archivedState.grown
    else io.rightVertexInput.executeGet3.state.grown

  val updateArea = new Area {
    val edgeRemaining = EdgeRemaining(
      leftGrownBits = leftGrownBits,
      rightGrownBits = rightGrownBits,
      weightBits = config.weightBits
    )
    edgeRemaining.io.leftGrown := io.leftVertexInput.executeGet3.state.grown
    edgeRemaining.io.rightGrown := io.rightVertexInput.executeGet3.state.grown
    edgeRemaining.io.weight := stages.executeGet3.state.weight
    stages.updateSet.remaining := edgeRemaining.io.remaining

    val edgeRemainingLayers = EdgeRemaining(
      leftGrownBits = leftGrownBits,
      rightGrownBits = rightGrownBits,
      weightBits = config.weightBits
    )
    edgeRemainingLayers.io.leftGrown := leftGrownUpdateLayers
    edgeRemainingLayers.io.rightGrown := rightGrownUpdateLayers
    edgeRemainingLayers.io.weight := stages.executeGet3.state.weight
    stages.updateSet.remainingVsElasticLayers := edgeRemainingLayers.io.remaining
  }

  stages.updateSet2.connect(stages.updateGet)

  stages.updateSet3.connect(stages.updateGet2)

  // Live edge response
  val edgeResponse = EdgeResponse(config.vertexBits, config.weightBits)
  edgeResponse.io.leftShadow := io.leftVertexInput.updateGet3.shadow
  edgeResponse.io.rightShadow := io.rightVertexInput.updateGet3.shadow
  edgeResponse.io.leftIsVirtual := io.leftVertexInput.updateGet3.state.isVirtual
  edgeResponse.io.rightIsVirtual := io.rightVertexInput.updateGet3.state.isVirtual
  edgeResponse.io.leftVertex := leftVertex
  edgeResponse.io.rightVertex := rightVertex
  edgeResponse.io.remaining := stages.updateGet3.remaining

  // Archived edge response (from layer-0 counterpart vertex's archived pipeline)
  val archivedEdgeResponse = EdgeResponse(config.vertexBits, config.weightBits)
  val leftL0Elastic = config.vertexHasElasticLayers(io.leftL0Vertex)
  val rightL0Elastic = config.vertexHasElasticLayers(io.rightL0Vertex)
  archivedEdgeResponse.io.leftShadow := (if (leftL0Elastic) io.leftL0VertexInput.updateGet3.archivedShadow
    else io.leftVertexInput.updateGet3.shadow)
  archivedEdgeResponse.io.rightShadow := (if (rightL0Elastic) io.rightL0VertexInput.updateGet3.archivedShadow
    else io.rightVertexInput.updateGet3.shadow)
  archivedEdgeResponse.io.leftIsVirtual := (if (leftL0Elastic) io.leftL0VertexInput.updateGet3.archivedState.isVirtual
    else io.leftVertexInput.updateGet3.state.isVirtual)
  archivedEdgeResponse.io.rightIsVirtual := (if (rightL0Elastic) io.rightL0VertexInput.updateGet3.archivedState.isVirtual
    else io.rightVertexInput.updateGet3.state.isVirtual)
  archivedEdgeResponse.io.leftVertex := leftVertex
  archivedEdgeResponse.io.rightVertex := rightVertex
  archivedEdgeResponse.io.remaining := stages.updateGet3.remainingVsElasticLayers

  // Accumulation registers for scan results
  val edgeLayer = config.edgeLayerOf(edgeIndex)
  val scanAddresses = edgeLayer.map(l => config.archiveScanAddressesOf(l)).getOrElse(Seq())

  val accMaxGrowable = Reg(ConvergecastMaxGrowable(config.weightBits))
  accMaxGrowable.length.init(accMaxGrowable.length.maxValue)
  val accConflict = Reg(ConvergecastConflict(config.vertexBits))
  accConflict.valid.init(False)
  val convergecastConflictBitsInit = B(0, config.vertexBits bits)
  accConflict.node1.init(convergecastConflictBitsInit)
  accConflict.node2.init(convergecastConflictBitsInit)
  accConflict.touch1.init(convergecastConflictBitsInit)
  accConflict.touch2.init(convergecastConflictBitsInit)
  accConflict.vertex1.init(convergecastConflictBitsInit)
  accConflict.vertex2.init(convergecastConflictBitsInit)

  // Reset accumulators when scan starts
  when(io.edgeScanActive && io.edgeScanIndex === 0) {
    accMaxGrowable.length := accMaxGrowable.length.maxValue
    accConflict.valid := False
  }

  // Accumulate during scan: check if current scan index is relevant to this edge's layer
  if (scanAddresses.nonEmpty) {
    // Pipeline delay: scan index fed at offload, result arrives at updateGet3 after executeLatency cycles.
    // Track the scan index through the pipeline to know which address produced the current result.
    val scanIndexPipelined = Delay(io.edgeScanIndex, config.executeLatency)
    val scanActivePipelined = Delay(io.edgeScanActive, config.executeLatency)

    val addressRelevant = scanAddresses.map(addr =>
      scanIndexPipelined === U(addr, config.archiveAddressBits bits)
    ).reduce(_ || _)

    when(scanActivePipelined && addressRelevant) {
      when(archivedEdgeResponse.io.maxGrowable.length < accMaxGrowable.length) {
        accMaxGrowable := archivedEdgeResponse.io.maxGrowable
      }
      when(!accConflict.valid && archivedEdgeResponse.io.conflict.valid) {
        accConflict := archivedEdgeResponse.io.conflict
      }
    }
  }

  // Capture live result when the actual instruction exits the pipeline (compact.valid = true).
  // During scan cycles, compact.valid is false so these hold the real result.
  val liveMaxGrowableReg = Reg(ConvergecastMaxGrowable(config.weightBits))
  liveMaxGrowableReg.length.init(liveMaxGrowableReg.length.maxValue)
  val liveConflictReg = Reg(ConvergecastConflict(config.vertexBits))
  liveConflictReg.valid.init(False)
  liveConflictReg.node1.init(convergecastConflictBitsInit)
  liveConflictReg.node2.init(convergecastConflictBitsInit)
  liveConflictReg.touch1.init(convergecastConflictBitsInit)
  liveConflictReg.touch2.init(convergecastConflictBitsInit)
  liveConflictReg.vertex1.init(convergecastConflictBitsInit)
  liveConflictReg.vertex2.init(convergecastConflictBitsInit)
  when(stages.updateGet3.compact.valid) {
    liveMaxGrowableReg := edgeResponse.io.maxGrowable
    liveConflictReg := edgeResponse.io.conflict
  }

  // Final output: min of captured live and accumulated archived
  val combinedMaxGrowable = ConvergecastMaxGrowable(config.weightBits)
  combinedMaxGrowable.length := Mux(
    liveMaxGrowableReg.length < accMaxGrowable.length,
    liveMaxGrowableReg.length,
    accMaxGrowable.length
  )
  val combinedConflict = ConvergecastConflict(config.vertexBits)
  when(accConflict.valid) {
    combinedConflict := accConflict
  } otherwise {
    combinedConflict := liveConflictReg
  }

  // 1 cycle delay when context is used (to ensure read latency >= execute latency)
  val outDelay = (config.contextDepth != 1).toInt
  io.maxGrowable := Delay(combinedMaxGrowable, outDelay)
  io.conflict := Delay(combinedConflict, outDelay)

  /* No write back: giving ports for external edge weights channels */
  // val writeState = stages.updateGet3.state
  // if (config.contextBits > 0) {
  //   ram.write(
  //     address = stages.updateGet3.compact.contextId,
  //     data = writeState,
  //     enable = stages.updateGet3.compact.valid
  //   )
  // } else {
  //   when(stages.updateGet3.compact.valid) {
  //     if (!config.hardCodeWeights) {
  //       register := writeState
  //     }
  //   }
  // }

  // inject registers
  for (stageName <- config.injectRegisters) {
    stages.injectRegisterAt(stageName)
  }
  stages.finish()

}

// sbt 'testOnly microblossom.modules.EdgeTest'
class EdgeTest extends AnyFunSuite {

  test("construct a Edge") {
    val config = DualConfig(filename = "./resources/graphs/example_code_capacity_d3.json")
    // config.contextDepth = 1024 // fit in a single Block RAM of 36 kbits in 36-bit mode
    config.contextDepth = 1 // no context switch
    config.sanityCheck()
    Config.spinal().generateVerilog(new Component {
      val edge = Edge(config, 0)
      edge.io.leftVertexInput.assignDontCare()
      edge.io.rightVertexInput.assignDontCare()
      edge.io.leftL0VertexInput.assignDontCare()
      edge.io.rightL0VertexInput.assignDontCare()
      edge.io.message.assignDontCare()
      edge.io.edgeScanActive := False
      edge.io.edgeScanIndex := U(0, config.archiveAddressBits bits)
    })
  }

}

// sbt "runMain microblossom.modules.EdgeEstimation"
object EdgeEstimation extends App {
  def dualConfig(name: String): DualConfig = {
    DualConfig(filename = s"./resources/graphs/example_$name.json"),
  }
  val configurations = List(
    // 7xLUT6, 1xLUT5, 3xLUT4 -> 11
    (dualConfig("code_capacity_d5"), 1, "code capacity 2 neighbors"),
    // 9xLUT6, 3xLUT4 -> 12
    (dualConfig("code_capacity_rotated_d5"), 12, "code capacity 4 neighbors"),
    // 6xLUT6, 4xLUT5, 5xLUT4, 1xLUT3 -> 16
    (dualConfig("phenomenological_rotated_d5"), 141, "phenomenological 6 neighbors"),
    // 19xLUT6, 9xLUT4, 5xLUT2, 1xCARRY4 -> 34
    (dualConfig("circuit_level_d5"), 365, "circuit-level 12 neighbors"),
    // 18xLUT6, 2xLUT5, 11xLUT4, 4xLUT2, 1xCARRY4 -> 36
    (dualConfig("circuit_level_d11"), 4719, "circuit-level 12 neighbors")
  )
  for ((config, edgeIndex, name) <- configurations) {
    val reports = Vivado.report(new Component {
      val edge = Edge(config, edgeIndex)
      edge.io.leftVertexInput.assignDontCare()
      edge.io.rightVertexInput.assignDontCare()
      edge.io.leftL0VertexInput.assignDontCare()
      edge.io.rightL0VertexInput.assignDontCare()
      edge.io.message.assignDontCare()
      edge.io.edgeScanActive := False
      edge.io.edgeScanIndex := U(0, config.archiveAddressBits bits)
    })
    println(s"$name:")
    reports.resource.primitivesTable.print()
  }
}
