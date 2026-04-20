package microblossom.modules

import spinal.core._
import spinal.lib._
import microblossom._
import microblossom.types._
import microblossom.stage._
import microblossom.combinatorial._
import microblossom.util.Vivado
import org.scalatest.funsuite.AnyFunSuite

object Vertex {
  def getStages(
      config: DualConfig,
      vertexIndex: Int
  ): Stages[
    StageOffloadVertex,
    StageOffloadVertex2,
    StageOffloadVertex3,
    StageOffloadVertex4,
    StageExecuteVertex,
    StageExecuteVertex2,
    StageExecuteVertex3,
    StageUpdateVertex,
    StageUpdateVertex2,
    StageUpdateVertex3
  ] = {
    Stages(
      offload = () => StageOffloadVertex(config, vertexIndex),
      offload2 = () => StageOffloadVertex2(config, vertexIndex),
      offload3 = () => StageOffloadVertex3(config, vertexIndex),
      offload4 = () => StageOffloadVertex4(config, vertexIndex),
      execute = () => StageExecuteVertex(config, vertexIndex),
      execute2 = () => StageExecuteVertex2(config, vertexIndex),
      execute3 = () => StageExecuteVertex3(config, vertexIndex),
      update = () => StageUpdateVertex(config, vertexIndex),
      update2 = () => StageUpdateVertex2(config, vertexIndex),
      update3 = () => StageUpdateVertex3(config, vertexIndex)
    )
  }
}

case class Vertex(
    config: DualConfig,
    vertexIndex: Int,
    elastic: Boolean = false,
    /// When true, `shiftDonorLive` is tied to `shiftSource` (standalone elaboration). `DistributedDual` sets false and wires donors.
    tieShiftDonorToSelf: Boolean = true
) extends Component {
  val io = new Bundle {
    val message = in(BroadcastMessage(config))
    // interaction I/O
    val stageOutputs = out(Vertex.getStages(config, vertexIndex).getStageOutput)
    val edgeInputs = in(
      Vec(
        for (edgeIndex <- config.incidentEdgesOf(vertexIndex))
          yield Edge.getStages(config).getStageOutput
      )
    )
    val offloaderInputs = in(
      Vec(
        for (offloaderIndex <- config.incidentOffloaderOf(vertexIndex))
          yield Offloader.getStages(config, offloaderIndex).getStageOutput
      )
    )
    var peerVertexInputsExecute3 = in(
      Vec(
        for (edgeIndex <- config.incidentEdgesOf(vertexIndex))
          yield Vertex.getStages(config, config.peerVertexOfEdge(edgeIndex, vertexIndex)).executeGet3
      )
    )
    /// Live vertex state before this cycle's register/RAM update (for `ArchiveElasticSlice` layer shift donors).
    val shiftSource = out(VertexState(config.vertexBits, config.grownBitsOf(vertexIndex)))
    /// Donor live state, wired in `DistributedDual` when this vertex shifts from a higher layer.
    val shiftDonorLive = in(VertexState(config.vertexBits, config.grownBitsOf(vertexIndex)))
    /** Central write address for ArchiveElasticSlice. */
    val archiveWriteAddr = in UInt (config.archiveAddressBits bits)
    /** True when data has shifted to layer 0 and should be committed to archive. */
    val archiveCommitEn = in(Bool())
    /** Edge scan: index into archivedRegs for pipeline input. */
    val scanIndex = in UInt (config.archiveAddressBits bits)
    /** Scan writeback: the scan index that just exited the pipeline, and enable. */
    val scanWritebackIndex = in UInt (config.archiveAddressBits bits)
    val scanWritebackEn = in(Bool())
    // True when this cycle's archivedPostExecute actually altered archivedState
    // (the captured instruction produced a change). Used by DistributedDual to
    // gate activeDepth advancement in the windowed-scan design.
    val archivedChanged = out(Bool())
    // True when this cycle's vertexPostExecuteState actually altered live state.
    // Used by DistributedDual to activate layer 1 in the windowed-scan design.
    val liveChanged = out(Bool())
    // final outputs
    val maxGrowable = out(ConvergecastMaxGrowable(config.weightBits))
  }

  val stages = Vertex.getStages(config, vertexIndex)
  stages.connectStageOutput(io.stageOutputs)

  // fetch
  var ram: Mem[VertexState] = null
  var layers: Mem[VertexState] = null
  var register = Reg(VertexState(config.vertexBits, config.grownBitsOf(vertexIndex)))
  register.init(VertexState.resetValue(config, vertexIndex))
  var fetchState = VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
  var message = BroadcastMessage(config)

  val elasticLayersDepth = if (elastic) config.archiveDepth else 1

  // Fetch: always from ram (context switching) or register (single context). Elastic does not read from layers.
  if (config.contextBits > 0) {
    ram = Mem(VertexState(config.vertexBits, config.grownBitsOf(vertexIndex)), config.contextDepth)
    ram.setTechnology(ramBlock)
    fetchState := ram.readSync(
      address = io.message.contextId,
      enable = io.message.valid
    )
    message := RegNext(io.message)
  } else {
    fetchState := register
    message := io.message
  }

  if (elastic) {
    layers = Mem(VertexState(config.vertexBits, config.grownBitsOf(vertexIndex)), elasticLayersDepth)
    layers.setTechnology(ramBlock)
  }

  // Register file holding all archived vertex states. Padded to at least 2 for address bit alignment.
  val archivedRegsDepth = config.archiveDepth max 2
  val archivedRegs =
    if (elastic) Vec.fill(archivedRegsDepth)(Reg(VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))))
    else null
  if (elastic) {
    for (i <- 0 until archivedRegsDepth) {
      archivedRegs(i).init(VertexState.resetValue(config, vertexIndex))
    }
  }

  // Simulation-only debug read port for `layers` BRAM.
  val layersDebugAddr = if (elastic) UInt(log2Up(elasticLayersDepth) max 1 bits) else null
  val layersDebugData = if (elastic) VertexState(config.vertexBits, config.grownBitsOf(vertexIndex)) else null
  if (elastic) {
    layersDebugData := layers.readAsync(layersDebugAddr)
  }

  val resetVertexState = VertexState.resetValue(config, vertexIndex)

  // Capture only state-modifying instructions for archived execute (constant during scan).
  val capturedMessage = Reg(BroadcastMessage(config))
  when(message.valid && message.instruction.needsArchivedScan()) {
    capturedMessage := message
  }

  stages.offloadSet.message := message
  stages.offloadSet.state := Mux(message.isReset, resetVertexState, fetchState)
  if (elastic) {
    stages.offloadSet.archivedState := archivedRegs(io.scanIndex)
  }

  stages.offloadSet2.connect(stages.offloadGet)

  stages.offloadSet3.connect(stages.offloadGet2)
  var offload3Area = new Area {
    var tightCounter = VertexTightCounter(
      numEdges = config.numIncidentEdgeOf(vertexIndex)
    )
    for (localIndex <- 0 until config.numIncidentEdgeOf(vertexIndex)) {
      tightCounter.io.tights(localIndex) :=
        io.edgeInputs(localIndex).offloadGet2.isTightExFusion
    }
    stages.offloadSet3.isUniqueTight := tightCounter.io.isUnique
    stages.offloadSet3.isIsolated := tightCounter.io.isIsolated
  }

  stages.offloadSet4.connect(stages.offloadGet3)

  stages.executeSet.connect(stages.offloadGet4)
  var executeArea = new Area {
    var offloadStalled = OffloadStalled(
      numConditions = config.numIncidentOffloaderOf(vertexIndex)
    )
    for (localIndex <- 0 until config.numIncidentOffloaderOf(vertexIndex)) {
      offloadStalled.io.conditions(localIndex) :=
        io.offloaderInputs(localIndex).offloadGet4.getVertexIsStalled(vertexIndex)
    }
    stages.executeSet.isStalled := offloadStalled.io.isStalled
  }

  stages.executeSet2.connect(stages.executeGet)
  var execute2Area = new Area {
    var vertexPostExecuteState = VertexPostExecuteState(
      config = config,
      vertexIndex = vertexIndex
    )
    vertexPostExecuteState.io.before := stages.executeGet.state
    vertexPostExecuteState.io.message := stages.executeGet.message
    vertexPostExecuteState.io.isStalled := stages.executeGet.isStalled
    stages.executeSet2.state := vertexPostExecuteState.io.after
    // liveChanged: True this cycle iff the live pipeline actually altered state
    // for this vertex. Only meaningful when an SM instruction is at this stage.
    io.liveChanged := stages.executeGet.message.valid &&
      (vertexPostExecuteState.io.after.asBits =/= vertexPostExecuteState.io.before.asBits)

    if (elastic) {
      // Apply captured instruction to archived state
      var archivedPostExecute = VertexPostExecuteState(
        config = config,
        vertexIndex = vertexIndex
      )
      archivedPostExecute.io.before := stages.executeGet.archivedState
      archivedPostExecute.io.message := capturedMessage
      if (config.vertexLayerId.contains(vertexIndex)) {
        archivedPostExecute.io.isStalled := stages.executeGet.archivedState.isVirtual
      } else {
        archivedPostExecute.io.isStalled := False
      }
      stages.executeSet2.archivedState := archivedPostExecute.io.after
      // archivedChanged is computed later at the writeback stage so it aligns with
      // scanWritebackEn / writebackTick (see below).
    } else {
      io.archivedChanged := False
    }
  }

  stages.executeSet3.connect(stages.executeGet2)

  stages.updateSet.connect(stages.executeGet3)
  var updateArea = new Area {
    var vertexPropagatingPeer = VertexPropagatingPeer(
      config = config,
      vertexIndex = vertexIndex
    )
    vertexPropagatingPeer.io.grown := stages.executeGet3.state.grown
    for (localIndex <- 0 until config.numIncidentEdgeOf(vertexIndex)) {
      vertexPropagatingPeer.io.edgeIsTight(localIndex) :=
        io.edgeInputs(localIndex).executeGet3.isTight
      vertexPropagatingPeer.io.peerSpeed(localIndex) := io.peerVertexInputsExecute3(localIndex).state.speed
      vertexPropagatingPeer.io.peerNode(localIndex) := io.peerVertexInputsExecute3(localIndex).state.node
      vertexPropagatingPeer.io.peerRoot(localIndex) := io.peerVertexInputsExecute3(localIndex).state.root
    }
    stages.updateSet.propagatingPeer := vertexPropagatingPeer.io.peer

    if (elastic) {
      // Archived peer propagation: same logic but from archivedState + archived tight signals.
      // Only peers that are also elastic have archivedState; others use reset defaults.
      var archivedPropagatingPeer = VertexPropagatingPeer(
        config = config,
        vertexIndex = vertexIndex
      )
      archivedPropagatingPeer.io.grown := stages.executeGet3.archivedState.grown
      for ((edgeIndex, localIndex) <- config.incidentEdgesOf(vertexIndex).zipWithIndex) {
        val peerVi = config.peerVertexOfEdge(edgeIndex, vertexIndex)
        val peerElastic = config.vertexHasElasticLayers(peerVi)
        archivedPropagatingPeer.io.edgeIsTight(localIndex) :=
          io.edgeInputs(localIndex).executeGet3.isTightVsElasticLayers
        if (peerElastic) {
          archivedPropagatingPeer.io.peerSpeed(localIndex) := io.peerVertexInputsExecute3(localIndex).archivedState.speed
          archivedPropagatingPeer.io.peerNode(localIndex) := io.peerVertexInputsExecute3(localIndex).archivedState.node
          archivedPropagatingPeer.io.peerRoot(localIndex) := io.peerVertexInputsExecute3(localIndex).archivedState.root
        } else {
          archivedPropagatingPeer.io.peerSpeed(localIndex) := Speed.Stay
          archivedPropagatingPeer.io.peerNode(localIndex) := B(config.IndexNone, config.vertexBits bits)
          archivedPropagatingPeer.io.peerRoot(localIndex) := B(config.IndexNone, config.vertexBits bits)
        }
      }
      stages.updateSet.archivedPropagatingPeer := archivedPropagatingPeer.io.peer
    }
  }

  stages.updateSet2.connect(stages.updateGet)
  var update2Area = new Area {
    // Live post-update
    var vertexPostUpdateState = VertexPostUpdateState(
      config = config,
      vertexIndex = vertexIndex
    )
    vertexPostUpdateState.io.before := stages.updateGet.state
    vertexPostUpdateState.io.propagator := stages.updateGet.propagatingPeer
    vertexPostUpdateState.io.holdForArchive :=
      stages.updateGet.compact.valid && stages.updateGet.compact.isArchiveElasticSlice
    stages.updateSet2.state := vertexPostUpdateState.io.after

    if (elastic) {
      // Archived post-update
      var archivedPostUpdateState = VertexPostUpdateState(
        config = config,
        vertexIndex = vertexIndex
      )
      archivedPostUpdateState.io.before := stages.updateGet.archivedState
      archivedPostUpdateState.io.propagator := stages.updateGet.archivedPropagatingPeer
      archivedPostUpdateState.io.holdForArchive := False
      stages.updateSet2.archivedState := archivedPostUpdateState.io.after
    }
  }

  stages.updateSet3.connect(stages.updateGet2)
  var update3Area = new Area {
    // Live shadow
    var vertexShadow = VertexShadow(
      config = config,
      vertexIndex = vertexIndex
    )
    vertexShadow.io.isVirtual := stages.updateGet2.state.isVirtual
    vertexShadow.io.node := stages.updateGet2.state.node
    vertexShadow.io.root := stages.updateGet2.state.root
    vertexShadow.io.speed := stages.updateGet2.state.speed
    vertexShadow.io.grown := stages.updateGet2.state.grown
    vertexShadow.io.isStalled := stages.updateGet2.isStalled
    vertexShadow.io.propagator := stages.updateGet2.propagatingPeer
    stages.updateSet3.shadow := vertexShadow.io.shadow

    if (elastic) {
      // Archived shadow
      var archivedVertexShadow = VertexShadow(
        config = config,
        vertexIndex = vertexIndex
      )
      archivedVertexShadow.io.isVirtual := stages.updateGet2.archivedState.isVirtual
      archivedVertexShadow.io.node := stages.updateGet2.archivedState.node
      archivedVertexShadow.io.root := stages.updateGet2.archivedState.root
      archivedVertexShadow.io.speed := stages.updateGet2.archivedState.speed
      archivedVertexShadow.io.grown := stages.updateGet2.archivedState.grown
      archivedVertexShadow.io.isStalled := False
      archivedVertexShadow.io.propagator := stages.updateGet2.archivedPropagatingPeer
      stages.updateSet3.archivedShadow := archivedVertexShadow.io.shadow
    }
  }

  val vertexResponse = VertexResponse(config, vertexIndex)
  vertexResponse.io.state := stages.updateGet3.state

  // Capture live vertex maxGrowable when the actual instruction exits (compact.valid).
  // During scan cycles compact.valid is false, so the register holds the real result.
  val liveVertexMaxGrowable = Reg(ConvergecastMaxGrowable(config.weightBits))
  liveVertexMaxGrowable.length.init(liveVertexMaxGrowable.length.maxValue)
  when(stages.updateGet3.compact.valid) {
    liveVertexMaxGrowable := vertexResponse.io.maxGrowable
  }

  // 1 cycle delay when context is used (to ensure read latency >= execute latency)
  val outDelay = (config.contextDepth != 1).toInt
  io.maxGrowable := Delay(liveVertexMaxGrowable, outDelay)

  // shift logic for both ram & register storage
  val writeState = stages.updateGet3.state
  val compact = stages.updateGet3.compact
  val commitArchive = compact.valid && compact.isArchiveElasticSlice
  val memWriteEnable = compact.valid && !compact.isArchiveElasticSlice
  val archShiftMode = config.archiveElasticLayerShiftModeOf(vertexIndex)
  val shiftWriteEnable = if (archShiftMode != 0) commitArchive else False
  val resetLiveState = VertexState.resetValue(config, vertexIndex)

  if (config.contextBits > 0) {
    val rs = ram.readAsync(compact.contextId)
    io.shiftSource.speed := rs.speed
    io.shiftSource.node := rs.node
    io.shiftSource.root := rs.root
    io.shiftSource.isVirtual := rs.isVirtual
    io.shiftSource.isDefect := rs.isDefect
    io.shiftSource.grown := rs.grown

    val ramWriteData = VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
    when(shiftWriteEnable) {
      if (archShiftMode == 1) {
        ramWriteData := io.shiftDonorLive
      } else {
        ramWriteData.speed := resetLiveState.speed
        ramWriteData.node := resetLiveState.node
        ramWriteData.root := resetLiveState.root
        ramWriteData.isVirtual := resetLiveState.isVirtual
        ramWriteData.isDefect := resetLiveState.isDefect
        ramWriteData.grown := resetLiveState.grown
      }
    } otherwise {
      ramWriteData := writeState
    }
    ram.write(
      address = compact.contextId,
      data = ramWriteData,
      enable = memWriteEnable || shiftWriteEnable
    )
    if (elastic) {
      val mAddrW = log2Up(elasticLayersDepth) max 1
      // Writeback from pipeline: archivedState exits at updateGet3, write to regs + BRAM.
      val wbData = stages.updateGet3.archivedState
      val wbAddr = io.scanWritebackIndex.resize(mAddrW)
      // archivedChanged: True this cycle iff this scan-writeback actually alters BRAM state.
      // Aligned with scanWritebackEn so DistributedDual can gate on writebackTick.
      io.archivedChanged := io.scanWritebackEn && (wbData.asBits =/= archivedRegs(wbAddr).asBits)
      when(io.scanWritebackEn) {
        archivedRegs(wbAddr) := wbData
        layers.write(address = wbAddr, data = wbData, enable = True)
      } elsewhen(commitArchive && io.archiveCommitEn) {
        // ArchiveElasticSlice: snapshot pre-shift state into archive (only after warmup)
        layers.write(address = io.archiveWriteAddr.resize(mAddrW), data = rs, enable = True)
        archivedRegs(io.archiveWriteAddr) := rs
      }
    }
  } else {
    io.shiftSource := register
    if (archShiftMode == 1) {
      when(commitArchive) {
        register := io.shiftDonorLive
      } elsewhen (memWriteEnable) {
        register := writeState
      }
    } else if (archShiftMode == 2) {
      when(commitArchive) {
        register.speed := resetLiveState.speed
        register.node := resetLiveState.node
        register.root := resetLiveState.root
        register.isVirtual := resetLiveState.isVirtual
        register.isDefect := resetLiveState.isDefect
        register.grown := resetLiveState.grown
      } elsewhen (memWriteEnable) {
        register := writeState
      }
    } else {
      when(memWriteEnable) {
        register := writeState
      }
    }
    if (elastic) {
      val mAddrW = log2Up(elasticLayersDepth) max 1
      // Writeback from pipeline: archivedState exits at updateGet3, write to regs + BRAM.
      val wbData = stages.updateGet3.archivedState
      val wbAddr = io.scanWritebackIndex.resize(mAddrW)
      // archivedChanged: True this cycle iff this scan-writeback actually alters BRAM state.
      // Aligned with scanWritebackEn so DistributedDual can gate on writebackTick.
      io.archivedChanged := io.scanWritebackEn && (wbData.asBits =/= archivedRegs(wbAddr).asBits)
      when(io.scanWritebackEn) {
        archivedRegs(wbAddr) := wbData
        layers.write(address = wbAddr, data = wbData, enable = True)
      } elsewhen(commitArchive && io.archiveCommitEn) {
        // ArchiveElasticSlice: snapshot pre-shift state into archive (only after warmup)
        layers.write(address = io.archiveWriteAddr.resize(mAddrW), data = register, enable = True)
        archivedRegs(io.archiveWriteAddr) := register
      }
    }
  }
  // debugging etc purposes
  if (tieShiftDonorToSelf) {
    io.shiftDonorLive.speed := io.shiftSource.speed
    io.shiftDonorLive.node := io.shiftSource.node
    io.shiftDonorLive.root := io.shiftSource.root
    io.shiftDonorLive.isVirtual := io.shiftSource.isVirtual
    io.shiftDonorLive.isDefect := io.shiftSource.isDefect
    io.shiftDonorLive.grown := io.shiftSource.grown
  }

  // inject registers
  for (stageName <- config.injectRegisters) {
    stages.injectRegisterAt(stageName)
  }
  stages.finish()


}

/** Wires dormant mirror-archive ports for standalone `Vertex` elaboration (e.g. Verilog export). */
case class VertexElaborationStub(
    config: DualConfig,
    vertexIndex: Int,
    elastic: Boolean = false,
    tieShiftDonorToSelf: Boolean = true
) extends Component {
  val inner = Vertex(config, vertexIndex, elastic, tieShiftDonorToSelf)
  inner.io.archiveWriteAddr := U(0, config.archiveAddressBits bits)
  inner.io.archiveCommitEn := False
  inner.io.scanIndex := U(0, config.archiveAddressBits bits)
  inner.io.scanWritebackIndex := U(0, config.archiveAddressBits bits)
  inner.io.scanWritebackEn := False
}

// sbt 'testOnly microblossom.modules.VertexTest'
class VertexTest extends AnyFunSuite {

  test("construct a Vertex") {
    val config = DualConfig(filename = "./resources/graphs/example_code_capacity_d3.json")
    // config.contextDepth = 1024 // fit in a single Block RAM of 36 kbits in 36-bit mode
    config.contextDepth = 1 // no context switch
    config.sanityCheck()
    Config.spinal().generateVerilog(VertexElaborationStub(config, 0))
  }

}

// sbt "runMain microblossom.modules.VertexEstimation"
object VertexEstimation extends App {
  def dualConfig(name: String): DualConfig = {
    DualConfig(filename = s"./resources/graphs/example_$name.json")
  }
  val configurations = List(
    // 33xLUT6, 21xLUT5, 7xLUT4, 6xLUT3, 7xLUT2 -> 74
    (dualConfig("code_capacity_d5"), 1, "code capacity 2 neighbors"),
    // 40xLUT6, 24xLUT5, 22xLUT4, 5xLUT3, 6xLUT2 -> 97
    (dualConfig("code_capacity_rotated_d5"), 10, "code capacity 4 neighbors"),
    // 37xLUT6, 73xLUT5, 21xLUT4, 10xLUT3, 8xLUT2 -> 149
    (dualConfig("phenomenological_rotated_d5"), 64, "phenomenological 6 neighbors"),
    // 42xLUT6, 107xLUT5, 31xLUT4, 18xLUT3, 8xLUT2, 2xCARRY4 -> 208
    (dualConfig("circuit_level_d5"), 63, "circuit-level 12 neighbors"),
    // 79xLUT6, 212xLUT5, 7xLUT4, 14xLUT3, 6xLUT2, 2xLUT1, 4xCARRY4 -> 324
    (dualConfig("circuit_level_d11"), 845, "circuit-level 12 neighbors (d=11)")
  )
  for ((config, vertexIndex, name) <- configurations) {
    val reports = Vivado.report(VertexElaborationStub(config, vertexIndex))
    println(s"$name:")
    reports.resource.primitivesTable.print()
  }
}
