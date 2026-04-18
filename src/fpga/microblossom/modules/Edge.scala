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

  val leftL0Vertex = config.layer0CounterpartOf(leftVertex)
  val rightL0Vertex = config.layer0CounterpartOf(rightVertex)

  val io = new Bundle {
    val message = in(BroadcastMessage(config))
    // interaction I/O
    val stageOutputs = out(Edge.getStages(config).getStageOutput)
    val leftVertexInput = in(Vertex.getStages(config, leftVertex).getStageOutput)
    val rightVertexInput = in(Vertex.getStages(config, rightVertex).getStageOutput)
    // Layer-0 counterpart vertex stage outputs (for archived state at pipeline stages)
    val leftL0VertexInput = in(Vertex.getStages(config, leftL0Vertex).getStageOutput)
    val rightL0VertexInput = in(Vertex.getStages(config, rightL0Vertex).getStageOutput)
    val edgeScanActive = in(Bool())
    val edgeScanFeeding = in(Bool())  // true only during feeding phase, not drain
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

  // Archived state comes from layer-0 counterpart vertex's archivedState in pipeline stages.
  // For fusion edges with same L0 counterpart: the "upper" endpoint reads the current archivedState,
  // the "lower" endpoint reads a 1-cycle delayed capture (from the previous scan tick).
  // Accumulation triggers at scanIndex+1 instead of scanIndex.
  val isFusionSameL0 = config.isFusionEdgeSameL0(edgeIndex)
  val leftIsUpper = isFusionSameL0 && (leftVertex == config.fusionEdgeUpperVertex(edgeIndex))
  val rightIsUpper = isFusionSameL0 && (rightVertex == config.fusionEdgeUpperVertex(edgeIndex))

  // For fusion edges: capture previous tick's archivedState from L0 counterpart (1-cycle delay).
  // The lower endpoint uses this captured value; the upper uses the current archivedState.
  // When `supportLayerFusion` is on (`config.fusionElasticTightUsesLiveVsArchived0`): upper uses L0 **live** grown, lower RegNext(archived grown)
  // — same stagger pattern as upper=current archived / lower=RegNext(archived) for archive-only fusion.
  // `l0Archived` is call-by-name: `archivedState` only exists on elastic vertex bundles (`elastic generate`).
  def archivedGrownAt(
      l0Elastic: Boolean,
      l0Archived: => VertexState,
      l0LiveGrown: UInt,
      graphEndpointLive: UInt,
      isUpper: Boolean
  ): UInt = {
    if (!l0Elastic) graphEndpointLive
    else if (config.fusionElasticTightUsesLiveVsArchived0 && isFusionSameL0) {
      if (isUpper) l0LiveGrown
      else RegNext(l0Archived.grown)
    } else if (!isFusionSameL0 || isUpper) l0Archived.grown
    else RegNext(l0Archived.grown)
  }

  val conditionedVertexIsVirtualArch = if (hasLayerFusion) {
    val conditionedVertex = config.edgeConditionedVertex(edgeIndex)
    val condL0 = config.layer0CounterpartOf(conditionedVertex)
    if (config.fusionElasticTightUsesLiveVsArchived0 && isFusionSameL0) {
      // Live-vs-archived mode: use live isVirtual since the upper endpoint uses live state.
      if (conditionedVertex == leftVertex) io.leftL0VertexInput.offloadGet.state.isVirtual
      else io.rightL0VertexInput.offloadGet.state.isVirtual
    } else if (config.vertexHasElasticLayers(condL0)) {
      if (conditionedVertex == leftVertex) io.leftL0VertexInput.offloadGet.archivedState.isVirtual
      else io.rightL0VertexInput.offloadGet.archivedState.isVirtual
    } else {
      if (conditionedVertex == leftVertex) io.leftVertexInput.offloadGet.state.isVirtual
      else io.rightVertexInput.offloadGet.state.isVirtual
    }
  } else {
    False
  }

  val leftGrownOffloadLayers = archivedGrownAt(
    config.vertexHasElasticLayers(leftL0Vertex),
    io.leftL0VertexInput.offloadGet.archivedState,
    io.leftL0VertexInput.offloadGet.state.grown,
    io.leftVertexInput.offloadGet.state.grown,
    leftIsUpper
  )
  val rightGrownOffloadLayers = archivedGrownAt(
    config.vertexHasElasticLayers(rightL0Vertex),
    io.rightL0VertexInput.offloadGet.archivedState,
    io.rightL0VertexInput.offloadGet.state.grown,
    io.rightVertexInput.offloadGet.state.grown,
    rightIsUpper
  )

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
    if (config.fusionElasticTightUsesLiveVsArchived0 && isFusionSameL0) {
      if (conditionedVertex == leftVertex) io.leftL0VertexInput.executeGet2.state.isVirtual
      else io.rightL0VertexInput.executeGet2.state.isVirtual
    } else if (config.vertexHasElasticLayers(condL0)) {
      if (conditionedVertex == leftVertex) io.leftL0VertexInput.executeGet2.archivedState.isVirtual
      else io.rightL0VertexInput.executeGet2.archivedState.isVirtual
    } else {
      if (conditionedVertex == leftVertex) io.leftVertexInput.executeGet2.state.isVirtual
      else io.rightVertexInput.executeGet2.state.isVirtual
    }
  } else {
    False
  }

  val leftGrownExecuteLayers = archivedGrownAt(
    config.vertexHasElasticLayers(leftL0Vertex),
    io.leftL0VertexInput.executeGet2.archivedState,
    io.leftL0VertexInput.executeGet2.state.grown,
    io.leftVertexInput.executeGet2.state.grown,
    leftIsUpper
  )
  val rightGrownExecuteLayers = archivedGrownAt(
    config.vertexHasElasticLayers(rightL0Vertex),
    io.rightL0VertexInput.executeGet2.archivedState,
    io.rightL0VertexInput.executeGet2.state.grown,
    io.rightVertexInput.executeGet2.state.grown,
    rightIsUpper
  )

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

  val leftGrownUpdateLayers = archivedGrownAt(
    config.vertexHasElasticLayers(leftL0Vertex),
    io.leftL0VertexInput.executeGet3.archivedState,
    io.leftL0VertexInput.executeGet3.state.grown,
    io.leftVertexInput.executeGet3.state.grown,
    leftIsUpper
  )
  val rightGrownUpdateLayers = archivedGrownAt(
    config.vertexHasElasticLayers(rightL0Vertex),
    io.rightL0VertexInput.executeGet3.archivedState,
    io.rightL0VertexInput.executeGet3.state.grown,
    io.rightVertexInput.executeGet3.state.grown,
    rightIsUpper
  )

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

  // Archived edge response (from layer-0 counterpart vertex's archived pipeline).
  // For fusion edges with same L0: lower endpoint uses captured (previous tick) shadow, upper uses current.
  val archivedEdgeResponse = EdgeResponse(config.vertexBits, config.weightBits)
  val leftL0Elastic = config.vertexHasElasticLayers(leftL0Vertex)
  val rightL0Elastic = config.vertexHasElasticLayers(rightL0Vertex)

  // Archived fields are call-by-name — they are absent (null) on non-elastic vertex stage bundles.
  def archivedShadowFor(
      l0Elastic: Boolean,
      l0ArchivedShadow: => VertexShadowResult,
      l0LiveShadow: => VertexShadowResult,
      liveEndpointShadow: => VertexShadowResult,
      isUpper: Boolean
  ): VertexShadowResult = {
    if (!l0Elastic) liveEndpointShadow
    else if (config.fusionElasticTightUsesLiveVsArchived0 && isFusionSameL0) {
      if (isUpper) l0LiveShadow
      else RegNext(l0ArchivedShadow)
    } else if (!isFusionSameL0 || isUpper) l0ArchivedShadow
    else RegNext(l0ArchivedShadow)
  }
  def archivedIsVirtualFor(
      l0Elastic: Boolean,
      l0ArchivedVirtual: => Bool,
      l0LiveVirtual: => Bool,
      liveEndpointVirtual: => Bool,
      isUpper: Boolean
  ): Bool = {
    if (!l0Elastic) liveEndpointVirtual
    else if (config.fusionElasticTightUsesLiveVsArchived0 && isFusionSameL0) {
      // For the upper (live) endpoint: force True so EdgeResponse's isAvailable check
      // returns false. Without this, a free live vertex (node=IndexNone, non-virtual)
      // suppresses archived conflicts, causing the Looper to hang at Grow(0).
      if (isUpper) True
      else RegNext(l0ArchivedVirtual)
    } else if (!isFusionSameL0 || isUpper) l0ArchivedVirtual
    else RegNext(l0ArchivedVirtual)
  }

  archivedEdgeResponse.io.leftShadow := archivedShadowFor(
    leftL0Elastic,
    io.leftL0VertexInput.updateGet3.archivedShadow,
    io.leftL0VertexInput.updateGet3.shadow,
    io.leftVertexInput.updateGet3.shadow,
    leftIsUpper
  )
  archivedEdgeResponse.io.rightShadow := archivedShadowFor(
    rightL0Elastic,
    io.rightL0VertexInput.updateGet3.archivedShadow,
    io.rightL0VertexInput.updateGet3.shadow,
    io.rightVertexInput.updateGet3.shadow,
    rightIsUpper
  )
  archivedEdgeResponse.io.leftIsVirtual := archivedIsVirtualFor(
    leftL0Elastic,
    io.leftL0VertexInput.updateGet3.archivedState.isVirtual,
    io.leftL0VertexInput.updateGet3.state.isVirtual,
    io.leftVertexInput.updateGet3.state.isVirtual,
    leftIsUpper
  )
  archivedEdgeResponse.io.rightIsVirtual := archivedIsVirtualFor(
    rightL0Elastic,
    io.rightL0VertexInput.updateGet3.archivedState.isVirtual,
    io.rightL0VertexInput.updateGet3.state.isVirtual,
    io.rightVertexInput.updateGet3.state.isVirtual,
    rightIsUpper
  )
  archivedEdgeResponse.io.leftVertex := leftVertex
  archivedEdgeResponse.io.rightVertex := rightVertex
  archivedEdgeResponse.io.remaining := stages.updateGet3.remainingVsElasticLayers

  // Accumulation registers for scan results.
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

  // Reset accumulators on the rising edge of scanActive (scan start).
  // Cannot use edgeScanIndex===0 because reversed scan order makes index 0 the last tick.
  //
  // ALSO reset on RESET instruction. Without this, once accConflict latches a valid conflict
  // it persists forever: the normal write path requires `!accConflict.valid` so it can never
  // overwrite itself, and the scan-start reset doesn't fire when firmware RESETs (since RESET
  // clears archiveValidCount to 0, preventing future scans from starting). Symptom: after
  // firmware's soft reset, find_obstacle keeps returning the stale accumulated conflict
  // because combinedConflict at line 510-514 prefers accConflict over liveConflictReg.
  val prevScanActive = RegNext(io.edgeScanActive, init = False)
  val isResetAtUpdate = stages.updateGet3.compact.valid && stages.updateGet3.compact.isReset
  when((io.edgeScanActive && !prevScanActive) || isResetAtUpdate) {
    accMaxGrowable.length := accMaxGrowable.length.maxValue
    accConflict.valid := False
    accConflict.node1 := convergecastConflictBitsInit
    accConflict.node2 := convergecastConflictBitsInit
    accConflict.touch1 := convergecastConflictBitsInit
    accConflict.touch2 := convergecastConflictBitsInit
    accConflict.vertex1 := convergecastConflictBitsInit
    accConflict.vertex2 := convergecastConflictBitsInit
  }

  // Accumulate during scan: check if current scan index is relevant to this edge's layer
  if (scanAddresses.nonEmpty) {
    // Pipeline delay: scan index fed at offload, result arrives at updateGet3 after executeLatency cycles.
    val scanIndexPipelined = Delay(io.edgeScanIndex, config.executeLatency)
    val scanFeedingPipelined = Delay(io.edgeScanFeeding, config.executeLatency)

    // For fusion-same-L0 edges, the lower endpoint is delayed by RegNext (1 tick).
    // The result for entry i arrives at tick i+1. Delay both the feeding gate and the
    // index so the accumulation fires at the correct tick with the correct address.
    val effectiveIndexPipelined = if (isFusionSameL0) RegNext(scanIndexPipelined) else scanIndexPipelined
    val effectiveFeedingPipelined = if (isFusionSameL0) RegNext(scanFeedingPipelined, init = False) else scanFeedingPipelined

    val addressRelevant = scanAddresses.map(addr =>
      effectiveIndexPipelined === U(addr, config.archiveAddressBits bits)
    ).reduce(_ || _)

    when(effectiveFeedingPipelined && addressRelevant) {
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
      edge.io.edgeScanFeeding := False
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
      edge.io.edgeScanFeeding := False
      edge.io.edgeScanIndex := U(0, config.archiveAddressBits bits)
    })
    println(s"$name:")
    reports.resource.primitivesTable.print()
  }
}
