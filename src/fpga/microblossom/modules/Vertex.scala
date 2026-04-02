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

  // One archived snapshot per context (written only on ArchiveElasticSlice).
  val elasticLayersDepth = if (elastic) config.contextDepth else 1

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

  stages.offloadSet.message := message
  stages.offloadSet.state := Mux(message.isReset, VertexState.resetValue(config, vertexIndex), fetchState)

  stages.offloadSet2.connect(stages.offloadGet)

  stages.offloadSet3.connect(stages.offloadGet2)
  var offload3Area = new Area {
    var tightCounter = VertexTightCounter(
      numEdges = config.numIncidentEdgeOf(vertexIndex)
    )
    for (localIndex <- 0 until config.numIncidentEdgeOf(vertexIndex)) {
      tightCounter.io.tights(localIndex) := io.edgeInputs(localIndex).offloadGet2.isTightExFusion
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
      vertexPropagatingPeer.io.edgeIsTight(localIndex) := io.edgeInputs(localIndex).executeGet3.isTight
      vertexPropagatingPeer.io.peerSpeed(localIndex) := io.peerVertexInputsExecute3(localIndex).state.speed
      vertexPropagatingPeer.io.peerNode(localIndex) := io.peerVertexInputsExecute3(localIndex).state.node
      vertexPropagatingPeer.io.peerRoot(localIndex) := io.peerVertexInputsExecute3(localIndex).state.root
    }
    stages.updateSet.propagatingPeer := vertexPropagatingPeer.io.peer
  }

  stages.updateSet2.connect(stages.updateGet)
  var update2Area = new Area {
    var vertexPostUpdateState = VertexPostUpdateState(
      config = config,
      vertexIndex = vertexIndex
    )
    vertexPostUpdateState.io.before := stages.updateGet.state
    vertexPostUpdateState.io.propagator := stages.updateGet.propagatingPeer
    // do not update vertex's data while shifting is happening
    vertexPostUpdateState.io.holdForArchive :=
      stages.updateGet.compact.valid && stages.updateGet.compact.isArchiveElasticSlice
    stages.updateSet2.state := vertexPostUpdateState.io.after

  }

  stages.updateSet3.connect(stages.updateGet2)
  var update3Area = new Area {
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
  }

  val vertexResponse = VertexResponse(config, vertexIndex)
  vertexResponse.io.state := stages.updateGet3.state

  // 1 cycle delay when context is used (to ensure read latency >= execute latency)
  val outDelay = (config.contextDepth != 1).toInt
  io.maxGrowable := Delay(vertexResponse.io.maxGrowable, outDelay)

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
      layers.write(
        address = compact.contextId.resize(log2Up(elasticLayersDepth)),
        data = rs,
        enable = commitArchive
      )
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
      layers.write(
        address = U(0, log2Up(elasticLayersDepth) bits),
        data = register,
        enable = commitArchive
      )
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

// sbt 'testOnly microblossom.modules.VertexTest'
class VertexTest extends AnyFunSuite {

  test("construct a Vertex") {
    val config = DualConfig(filename = "./resources/graphs/example_code_capacity_d3.json")
    // config.contextDepth = 1024 // fit in a single Block RAM of 36 kbits in 36-bit mode
    config.contextDepth = 1 // no context switch
    config.sanityCheck()
    Config.spinal().generateVerilog(Vertex(config, 0))
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
    val reports = Vivado.report(Vertex(config, vertexIndex))
    println(s"$name:")
    reports.resource.primitivesTable.print()
  }
}
