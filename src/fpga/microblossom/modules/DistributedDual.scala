package microblossom.modules

import io.circe._
import io.circe.generic.extras._
import io.circe.generic.semiauto._
import spinal.core._
import spinal.lib._
import spinal.core.sim._
import microblossom._
import microblossom.util._
import microblossom.types._
import microblossom.util.Vivado
import org.scalatest.funsuite.AnyFunSuite
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

case class DistributedDual(config: DualConfig, ioConfig: DualConfig) extends Component {
  ioConfig.contextDepth = config.contextDepth

  val io = new Bundle {
    val message = in(BroadcastMessage(ioConfig, explicitReset = false))

    val maxGrowable = out(ConvergecastMaxGrowable(ioConfig.weightBits))
    val conflict = out(ConvergecastConflict(ioConfig.vertexBits))
    val parityReports = out(Bits(config.parityReportersNum bits))
  }

  // width conversion
  val broadcastMessage = BroadcastMessage(config)
  broadcastMessage.instruction.resizedFrom(io.message.instruction)
  broadcastMessage.valid := io.message.valid
  if (config.contextBits > 0) { broadcastMessage.contextId := io.message.contextId }
  broadcastMessage.isReset := io.message.instruction.isReset

  // delay the signal so that the synthesizer can automatically balancing the registers
  val broadcastRegInserted = Delay(broadcastMessage, config.broadcastDelay)
  // broadcastRegInserted.addAttribute("keep")
  // broadcastRegInserted.addAttribute("mark_debug = \"true\"")
  // reduce congestion by using global clocking BUFG to propagate the signal
  broadcastRegInserted.addAttribute("clock_buffer_type = \"BUFG\"")

  // instantiate vertices, edges and offloaders
  // first layer (t=0) vertices are elastic; the rest are not
  val firstLayerVertexIndices =
    if (config.numLayers > 0) config.layerFusion.layers(0).map(_.toInt).toSet
    else Set.empty[Int]
  val vertices = Seq
    .range(0, config.vertexNum)
    .map(vertexIndex =>
      new Vertex(
        config,
        vertexIndex,
        elastic = firstLayerVertexIndices(vertexIndex),
        tieShiftDonorToSelf = false
      )
    )
  val edges = Seq
    .range(0, config.edgeNum)
    .map(edgeIndex => new Edge(config, edgeIndex))
  val offloaders = Seq
    .range(0, config.offloaderNum)
    .map(offloaderIndex => new Offloader(config, offloaderIndex))

  // connect vertex I/O
  for ((vertex, vertexIndex) <- vertices.zipWithIndex) {
    vertex.io.message := broadcastRegInserted
    for ((edgeIndex, localIndex) <- config.incidentEdgesOf(vertexIndex).zipWithIndex) {
      vertex.io.edgeInputs(localIndex) := edges(edgeIndex).io.stageOutputs
    }
    for ((offloaderIndex, localIndex) <- config.incidentOffloaderOf(vertexIndex).zipWithIndex) {
      vertex.io.offloaderInputs(localIndex) := offloaders(offloaderIndex).io.stageOutputs
    }
    for ((edgeIndex, localIndex) <- config.incidentEdgesOf(vertexIndex).zipWithIndex) {
      vertex.io.peerVertexInputsExecute3(localIndex) := vertices(
        config.peerVertexOfEdge(edgeIndex, vertexIndex)
      ).io.stageOutputs.executeGet3
    }
    // `ArchiveElasticSlice`: layer shift donors (strictly higher layer → lower); unused vertices tie to self.
    if (config.archiveElasticLayerShiftModeOf(vertexIndex) == 1) {
      val donorVi = config.archiveElasticLayerShiftDonorOf(vertexIndex)
      val dVert = vertices(donorVi)
      vertex.io.shiftDonorLive.speed := dVert.io.shiftSource.speed
      vertex.io.shiftDonorLive.node := dVert.io.shiftSource.node
      vertex.io.shiftDonorLive.root := dVert.io.shiftSource.root
      vertex.io.shiftDonorLive.isVirtual := dVert.io.shiftSource.isVirtual
      vertex.io.shiftDonorLive.isDefect := dVert.io.shiftSource.isDefect
      vertex.io.shiftDonorLive.grown := dVert.io.shiftSource.grown.resize(vertex.io.shiftDonorLive.grown.getWidth bits)
    } else {
      vertex.io.shiftDonorLive.speed := vertex.io.shiftSource.speed
      vertex.io.shiftDonorLive.node := vertex.io.shiftSource.node
      vertex.io.shiftDonorLive.root := vertex.io.shiftSource.root
      vertex.io.shiftDonorLive.isVirtual := vertex.io.shiftSource.isVirtual
      vertex.io.shiftDonorLive.isDefect := vertex.io.shiftSource.isDefect
      vertex.io.shiftDonorLive.grown := vertex.io.shiftSource.grown
    }
  }

  // connect edge I/O
  for ((edge, edgeIndex) <- edges.zipWithIndex) {
    edge.io.message := broadcastRegInserted
    val (leftVertex, rightVertex) = config.incidentVerticesOf(edgeIndex)
    edge.io.leftVertexInput := vertices(leftVertex).io.stageOutputs
    edge.io.rightVertexInput := vertices(rightVertex).io.stageOutputs
  }

  // connect offloader I/O
  for ((offloader, offloaderIndex) <- offloaders.zipWithIndex) {
    for ((vertexIndex, localIndex) <- config.offloaderNeighborVertexIndices(offloaderIndex).zipWithIndex) {
      offloader.io.vertexInputsOffloadGet3(localIndex) := vertices(vertexIndex).io.stageOutputs.offloadGet3
    }
    for ((edgeIndex, localIndex) <- config.offloaderNeighborEdgeIndices(offloaderIndex).zipWithIndex) {
      offloader.io.neighborEdgeInputsOffloadGet3(localIndex) := edges(edgeIndex).io.stageOutputs.offloadGet3
    }
    val edgeIndex = config.offloaderEdgeIndex(offloaderIndex)
    offloader.io.edgeInputOffloadGet3 := edges(edgeIndex).io.stageOutputs.offloadGet3
  }

  // build convergecast tree for maxGrowable
  val maxGrowableConvergcastTree =
    Vec.fill(config.graph.vertex_edge_binary_tree.nodes.length)(ConvergecastMaxGrowable(config.weightBits))
  for ((treeNode, index) <- config.graph.vertex_edge_binary_tree.nodes.zipWithIndex) {
    if (index < config.vertexNum) {
      val vertexIndex = index
      maxGrowableConvergcastTree(index) := vertices(vertexIndex).io.maxGrowable
    } else if (index < config.vertexNum + config.edgeNum) {
      val edgeIndex = index - config.vertexNum
      maxGrowableConvergcastTree(index) := edges(edgeIndex).io.maxGrowable
    } else {
      val left = maxGrowableConvergcastTree(treeNode.l.get.toInt)
      val right = maxGrowableConvergcastTree(treeNode.r.get.toInt)
      when(left.length < right.length) {
        maxGrowableConvergcastTree(index) := left
      } otherwise {
        maxGrowableConvergcastTree(index) := right
      }
    }
  }

  val selectedMaxGrowable =
    Delay(maxGrowableConvergcastTree(config.graph.vertex_edge_binary_tree.nodes.length - 1), config.convergecastDelay)
  io.maxGrowable.resizedFrom(selectedMaxGrowable)

  // build convergecast tree of conflict
  val conflictConvergecastTree =
    Vec.fill(config.graph.edge_binary_tree.nodes.length)(ConvergecastConflict(config.vertexBits))
  for ((treeNode, index) <- config.graph.edge_binary_tree.nodes.zipWithIndex) {
    if (index < config.edgeNum) {
      val edgeIndex = index
      conflictConvergecastTree(index) := edges(edgeIndex).io.conflict
    } else {
      val left = conflictConvergecastTree(treeNode.l.get.toInt)
      val right = conflictConvergecastTree(treeNode.r.get.toInt)
      when(left.valid) {
        conflictConvergecastTree(index) := left
      } otherwise {
        conflictConvergecastTree(index) := right
      }
    }
  }
  val convergecastedConflict =
    Delay(conflictConvergecastTree(config.graph.edge_binary_tree.nodes.length - 1), config.convergecastDelay)
  io.conflict.resizedFrom(convergecastedConflict)

  // build convergecast tree of parity reporter
  for ((parityReporter, index) <- config.parityReporters.zipWithIndex) {
    val parities = Vec.fill(parityReporter.length)(Bool)
    for ((offloaderIndex, reportIndex) <- parityReporter.zipWithIndex) {
      if (offloaderIndex < offloaders.length) {
        parities(reportIndex) := offloaders(offloaderIndex.toInt).io.condition
      } else {
        parities(reportIndex) := False
      }
    }
    val parityReport = parities.reduceBalancedTree(_ ^ _) // XOR
    io.parityReports(index) := Delay(parityReport, config.convergecastDelay)
  }

  def simExecute(instruction: Long): (DataMaxGrowable, DataConflictRaw) = {
    io.message.valid #= true
    io.message.instruction #= instruction
    clockDomain.waitSampling()
    io.message.valid #= false
    for (idx <- 0 until config.readLatency - 1) { clockDomain.waitSampling() }
    sleep(1)
    (
      DataMaxGrowable(io.maxGrowable.length.toInt),
      // the distributed dual does not do node reordering, so leave the original index here
      DataConflictRaw(
        io.conflict.valid.toBoolean,
        io.conflict.node1.toInt,
        io.conflict.node2.toInt,
        io.conflict.touch1.toInt,
        io.conflict.touch2.toInt,
        io.conflict.vertex1.toInt,
        io.conflict.vertex2.toInt
      )
    )
  }

  // before compiling the simulator, mark the fields as public to enable snapshot
  def simMakePublicSnapshot() = {
    vertices.foreach(vertex => {
      vertex.register.simPublic()
      vertex.io.simPublic()
      if (vertex.elastic) {
        vertex.layersDebugAddr.simPublic()
        vertex.layersDebugData.simPublic()
      }
    })
    edges.foreach(edge => {
      if (!config.hardCodeWeights) {
        edge.register.simPublic()
      }
      edge.io.simPublic()
    })
    offloaders.foreach(offloader => {
      offloader.io.simPublic()
    })
  }

  // a temporary solution without primal offloading
  def simFindObstacle(maxGrowth: Long): (DataMaxGrowable, DataConflictRaw, Long) = {
    var (maxGrowable, conflict) = simExecute(ioConfig.instructionSpec.generateFindObstacle())
    var grown = 0.toLong
    breakable {
      while (maxGrowable.length > 0 && !conflict.valid) {
        var length = maxGrowable.length.toLong
        if (length == ioConfig.LengthNone) {
          break
        }
        if (length + grown > maxGrowth) {
          length = maxGrowth - grown
        }
        if (length == 0) {
          break
        }
        grown += length
        simExecute(ioConfig.instructionSpec.generateGrow(length))
        val update = simExecute(ioConfig.instructionSpec.generateFindObstacle())
        maxGrowable = update._1
        conflict = update._2
      }
    }
    (maxGrowable, conflict, grown)
  }

  // take a snapshot of the dual module, in the format of fusion blossom visualization
  def simSnapshot(abbrev: Boolean = true): Json = {
    // https://circe.github.io/circe/api/io/circe/JsonObject.html
    var jsonVertices = ArrayBuffer[Json]()
    vertices.foreach(vertex => {
      val register = vertex.register
      val vertexMap = Map(
        (if (abbrev) { "v" }
         else { "is_virtual" }) -> Json.fromBoolean(register.isVirtual.toBoolean),
        (if (abbrev) { "s" }
         else { "is_defect" }) -> Json.fromBoolean(register.isDefect.toBoolean)
      )
      val node = register.node.toLong
      if (node != config.IndexNone) {
        vertexMap += ((
          if (abbrev) { "p" }
          else { "propagated_dual_node" },
          Json.fromLong(node)
        ))
      }
      val root = register.root.toLong
      if (root != config.IndexNone) {
        vertexMap += ((
          if (abbrev) { "pg" }
          else { "propagated_grandson_dual_node" },
          Json.fromLong(root)
        ))
      }
      jsonVertices.append(Json.fromFields(vertexMap))
    })
    var jsonEdges = ArrayBuffer[Json]()
    edges.foreach(edge => {
      val register = edge.register
      val weight = if (config.hardCodeWeights) { config.graph.weighted_edges(edge.edgeIndex).w.toLong }
      else { register.weight.toLong }
      val (leftIndex, rightIndex) = config.incidentVerticesOf(edge.edgeIndex)
      val leftReg = vertices(leftIndex).register
      val rightReg = vertices(rightIndex).register
      val edgeMap = Map(
        (if (abbrev) { "w" }
         else { "weight" }) -> Json.fromLong(weight),
        (if (abbrev) { "l" }
         else { "left" }) -> Json.fromLong(leftIndex),
        (if (abbrev) { "r" }
         else { "right" }) -> Json.fromLong(rightIndex),
        (if (abbrev) { "lg" }
         else { "left_growth" }) -> Json.fromLong(leftReg.grown.toLong),
        (if (abbrev) { "rg" }
         else { "right_growth" }) -> Json.fromLong(rightReg.grown.toLong)
      )
      val leftNode = leftReg.node.toLong
      if (leftNode != config.IndexNone) {
        edgeMap += ((
          if (abbrev) { "ld" }
          else { "left_dual_node" },
          Json.fromLong(leftNode)
        ))
      }
      val leftRoot = leftReg.root.toLong
      if (leftRoot != config.IndexNone) {
        edgeMap += ((
          if (abbrev) { "lgd" }
          else { "left_grandson_dual_node" },
          Json.fromLong(leftRoot)
        ))
      }
      val rightNode = rightReg.node.toLong
      if (rightNode != config.IndexNone) {
        edgeMap += ((
          if (abbrev) { "rd" }
          else { "right_dual_node" },
          Json.fromLong(rightNode)
        ))
      }
      val rightRoot = rightReg.root.toLong
      if (rightRoot != config.IndexNone) {
        edgeMap += ((
          if (abbrev) { "rgd" }
          else { "right_grandson_dual_node" },
          Json.fromLong(rightRoot)
        ))
      }
      jsonEdges.append(Json.fromFields(edgeMap))
    })
    Json.fromFields(
      Map(
        "vertices" -> Json.fromValues(jsonVertices),
        "edges" -> Json.fromValues(jsonEdges)
      )
    )
  }

  def simMakePublicPreMatching() = {
    vertices.foreach(vertex => {
      vertex.register.node.simPublic()
      vertex.register.root.simPublic()
    })
    offloaders.foreach(offloader => {
      offloader.io.stageOutputs.offloadGet4.condition.simPublic()
    })
  }

  def simPreMatchings(): Seq[DataPreMatching] = {
    var preMatchings = ArrayBuffer[DataPreMatching]()
    for ((offloader, offloaderIndex) <- offloaders.zipWithIndex) {
      val edgeIndex = config.offloaderEdgeIndex(offloaderIndex)
      if (offloader.io.stageOutputs.offloadGet4.condition.toBoolean) {
        var pairIndex = config.incidentVerticesOf(edgeIndex)
        var pair = (vertices(pairIndex._1).register, vertices(pairIndex._2).register)
        if (pair._1.node.toLong == config.IndexNone) {
          pair = pair.swap
          pairIndex = pairIndex.swap
        }
        val node1 = pair._1.node.toInt
        val node2 = pair._2.node.toInt
        val touch1 = pair._1.root.toInt
        val touch2 = pair._2.root.toInt
        var vertex1 = pairIndex._1
        var vertex2 = pairIndex._2
        assert(node1 != config.IndexNone)
        assert(touch1 != config.IndexNone)
        assert(vertex1 != config.IndexNone)
        assert(vertex2 != config.IndexNone)
        val option_node2: Option[Int] = if (node2 == config.IndexNone) { None }
        else { Some(node2) }
        val option_touch2: Option[Int] = if (touch2 == config.IndexNone) { None }
        else { Some(touch2) }
        preMatchings.append(
          DataPreMatching(edgeIndex, node1, option_node2, touch1, option_touch2, vertex1, vertex2)
        )
      }
    }
    preMatchings
  }
}

// sbt 'testOnly microblossom.modules.DistributedDualTest'
class DistributedDualTest extends AnyFunSuite {

  test("construct a DistributedDual") {
    val config = DualConfig(filename = "./resources/graphs/example_code_capacity_d3.json")
    val ioConfig = DualConfig()
    // config.contextDepth = 1024 // fit in a single Block RAM of 36 kbits in 36-bit mode
    config.contextDepth = 1 // no context switch
    config.sanityCheck()
    Config.spinal().generateVerilog(DistributedDual(config, ioConfig))
  }

  test("test pipeline registers") {
    // gtkwave simWorkspace/DistributedDual/testA.fst
    val config = DualConfig(filename = "./resources/graphs/example_code_capacity_d3.json", minimizeBits = false)
    val ioConfig = DualConfig()
    config.graph.offloading = Seq() // remove all offloaders
    config.fitGraph(minimizeBits = false)
    config.sanityCheck()
    Config.sim
      .compile({
        val dut = DistributedDual(config, ioConfig)
        dut.simMakePublicSnapshot()
        dut
      })
      .doSim("testA") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)

        for (idx <- 0 to 10) { dut.clockDomain.waitSampling() }

        dut.simExecute(ioConfig.instructionSpec.generateReset())
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(0, 0))
        var (maxGrowable, conflict) = dut.simExecute(ioConfig.instructionSpec.generateFindObstacle())

        assert(maxGrowable.length == 2) // at most grow 2
        assert(conflict.valid == false)

        dut.simExecute(ioConfig.instructionSpec.generateGrow(1))
        var (maxGrowable2, conflict2) = dut.simExecute(ioConfig.instructionSpec.generateFindObstacle())
        assert(maxGrowable2.length == 1)
        assert(conflict2.valid == false)

        val (_, conflict3, grown3) = dut.simFindObstacle(1)
        assert(grown3 == 1)
        assert(conflict3.valid == true)
        assert(conflict3.node1 == 0)
        assert(conflict3.node2 == ioConfig.IndexNone)
        assert(conflict3.touch1 == 0)
        assert(conflict3.touch2 == ioConfig.IndexNone)
        assert(conflict3.vertex1 == 0)
        assert(conflict3.vertex2 == 3)

        println(dut.simSnapshot().noSpacesSortKeys)

        for (idx <- 0 to 10) { dut.clockDomain.waitSampling() }

      }
    }
  //  two consecutive ArchiveElasticSlice on phenomenological d3
  test("chain shift") {
    // Uses phenomenological_rotated_d3 with supportLayerFusion to exercise the layer shift chain.
    // Graph columns (same positional index across layers):
    //   Layer 0 (elastic): v0,  v3,  v4,  v7   — shift mode 1 (copy from layer 1)
    //   Layer 1:           v8,  v11, v12, v15   — shift mode 1 (copy from layer 2)
    //   Layer 2:           v16, v19, v20, v23   — shift mode 1 (copy from layer 3)
    //   Layer 3 (top):     v24, v27, v28, v31   — shift mode 2 (reset)
    //
    // After the first ArchiveElasticSlice, each vertex takes its donor's state:
    //   v0  ← v8's old state, v8  ← v16's old state, v16 ← v24's old state, v24 ← reset
    // After the second ArchiveElasticSlice, the chain shifts again:
    //   v0  ← v8's state (which was v16's old), v8 ← v16's (which was v24's from round 1), etc.
    //
    // gtkwave simWorkspace/DistributedDual/testChainShift.fst
    val config = DualConfig(
      filename = "./resources/graphs/example_phenomenological_rotated_d3.json",
      minimizeBits = false
    )
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq() // remove all offloaders
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 1
    config.sanityCheck()
    Config.sim
      .compile({
        val dut = DistributedDual(config, ioConfig)
        dut.simMakePublicSnapshot()
        dut
      })
      .doSim("testChainShift") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)

        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        // --- Round 1: place defect on top-layer vertex v24 and grow ---
        dut.simExecute(ioConfig.instructionSpec.generateReset())
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(24, 0))
        // LoadDefectsExternal clears isVirtual for layer-fusion vertices so they can grow.
        // v24 is in layer 3 (layerId=3).
        dut.simExecute(ioConfig.instructionSpec.generateLoadDefectsExternal(3))
        dut.simExecute(ioConfig.instructionSpec.generateGrow(2))

        // Verify pre-shift: v24 has node=0, grown=2, isDefect=true
        sleep(1)
        assert(dut.vertices(24).register.node.toLong == 0, "v24 node should be 0 before first shift")
        assert(dut.vertices(24).register.grown.toLong == 2, "v24 grown should be 2 before first shift")
        assert(dut.vertices(24).register.isDefect.toBoolean, "v24 should be defect before first shift")
        // v16 should still be in reset state (virtual, no node)
        assert(dut.vertices(16).register.node.toLong == config.IndexNone, "v16 node should be IndexNone before first shift")
        assert(dut.vertices(16).register.grown.toLong == 0, "v16 grown should be 0 before first shift")

        // First ArchiveElasticSlice: shifts v24→v16, v16→v8, v8→v0; v24 resets
        dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())

        sleep(1)
        // v16 should now hold v24's old state (node=0, grown=2, isDefect=true)
        assert(dut.vertices(16).register.node.toLong == 0, "v16 node should be 0 after first shift (was v24's)")
        assert(dut.vertices(16).register.grown.toLong == 2, "v16 grown should be 2 after first shift (was v24's)")
        assert(dut.vertices(16).register.isDefect.toBoolean, "v16 should be defect after first shift")
        // v24 should be reset (top layer → mode 2)
        assert(dut.vertices(24).register.node.toLong == config.IndexNone, "v24 node should be IndexNone after first shift (reset)")
        assert(dut.vertices(24).register.grown.toLong == 0, "v24 grown should be 0 after first shift (reset)")
        assert(!dut.vertices(24).register.isDefect.toBoolean, "v24 should not be defect after first shift (reset)")
        // v8 should hold v16's old state (which was reset/virtual before the shift)
        assert(dut.vertices(8).register.node.toLong == config.IndexNone, "v8 node should be IndexNone after first shift (was v16's reset)")
        assert(dut.vertices(8).register.grown.toLong == 0, "v8 grown should be 0 after first shift")

        // --- Round 2: place new defect on v24 and grow by 1 ---
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(24, 1))
        // v24 was reset by the shift (mode 2), so isVirtual=true again. Un-virtualize it.
        dut.simExecute(ioConfig.instructionSpec.generateLoadDefectsExternal(3))
        dut.simExecute(ioConfig.instructionSpec.generateGrow(1))

        sleep(1)
        assert(dut.vertices(24).register.node.toLong == 1, "v24 node should be 1 before second shift")
        assert(dut.vertices(24).register.grown.toLong == 1, "v24 grown should be 1 before second shift")

        // Second ArchiveElasticSlice: shifts again
        dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())

        sleep(1)
        // v8 should now hold what v16 had before the second shift.
        // Before the second shift, v16 had: node=0, grown=2+1=3 (the Grow(1) also applied to v16 since it has speed=Grow from round 1).
        // Actually, v16's grown depends on whether its speed was set to Grow.
        // After the first shift, v16 inherited v24's state: node=0, isDefect=true. The execute stage would have
        // set speed=Grow because it's a defect. So Grow(1) increments v16.grown from 2 to 3.
        assert(dut.vertices(8).register.node.toLong == 0, "v8 node should be 0 after second shift (was v16's, originally v24 round 1)")
        assert(dut.vertices(8).register.grown.toLong == 3, "v8 grown should be 3 after second shift (v16 had 2 + grew 1)")

        // v16 should now hold v24's round-2 state: node=1, grown=1
        assert(dut.vertices(16).register.node.toLong == 1, "v16 node should be 1 after second shift (was v24 round 2)")
        assert(dut.vertices(16).register.grown.toLong == 1, "v16 grown should be 1 after second shift (was v24 round 2)")

        // v24 reset again
        assert(dut.vertices(24).register.node.toLong == config.IndexNone, "v24 should be reset after second shift")
        assert(dut.vertices(24).register.grown.toLong == 0, "v24 grown should be 0 after second shift")

        // v0 (elastic, layer 0) should hold what v8 had before the second shift.
        // Before the second shift, v8 had the reset state (node=IndexNone, grown=0) from the first shift.
        // But Grow(1) may have been applied if v8's speed was Grow. Since v8 had no node (IndexNone),
        // speed should be Stay, so grown stays 0.
        assert(dut.vertices(0).register.node.toLong == config.IndexNone,
          "v0 node should be IndexNone after second shift (was v8's reset state)")
        assert(dut.vertices(0).register.grown.toLong == 0,
          "v0 grown should be 0 after second shift (v8 had no node, speed=Stay)")

        // Non-column virtual vertex v1 should be completely unaffected
        assert(dut.vertices(1).register.isVirtual.toBoolean, "v1 should still be virtual (non-column, unaffected)")

        println("Chain shift test passed")
        println(dut.simSnapshot().noSpacesSortKeys)

        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

  test("elastic layers RAM") {
    // Verifies that ArchiveElasticSlice writes the elastic vertex's pre-shift live state
    // into its `layers` BRAM by reading it back via a simulation-only debug read port.
    //
    // gtkwave simWorkspace/DistributedDual/testLayersCommit.fst
    val config = DualConfig(
      filename = "./resources/graphs/example_phenomenological_rotated_d3.json",
      minimizeBits = false
    )
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq()
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 1
    config.sanityCheck()

    Config.sim
      .compile({
        val dut = DistributedDual(config, ioConfig)
        dut.simMakePublicSnapshot()
        dut
      })
      .doSim("testLayersCommit") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)

        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        // Setup: make v0 a defect with some growth
        dut.simExecute(ioConfig.instructionSpec.generateReset())
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(0, 0))
        dut.simExecute(ioConfig.instructionSpec.generateLoadDefectsExternal(0))
        dut.simExecute(ioConfig.instructionSpec.generateGrow(1))

        // Verify live state before archive
        sleep(1)
        assert(dut.vertices(0).register.node.toLong == 0, "v0 node should be 0 before archive")
        assert(dut.vertices(0).register.grown.toLong == 1, "v0 grown should be 1 before archive")
        assert(dut.vertices(0).register.isDefect.toBoolean, "v0 should be defect before archive")

        // Archive: v0's state (node=0, grown=1) -> layers[0], v0.register <- v8's reset state
        dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())
        sleep(1)

        // v0's live register should now hold v8's old state (reset)
        assert(dut.vertices(0).register.node.toLong == config.IndexNone,
          "v0 register node should be IndexNone after archive (shifted from v8)")
        assert(dut.vertices(0).register.grown.toLong == 0,
          "v0 register grown should be 0 after archive")

        // Read layers[0] via the debug read port
        dut.vertices(0).layersDebugAddr #= 0
        sleep(1)
        val layNode = dut.vertices(0).layersDebugData.node.toLong
        val layRoot = dut.vertices(0).layersDebugData.root.toLong
        val layDefect = dut.vertices(0).layersDebugData.isDefect.toBoolean
        val layGrown = dut.vertices(0).layersDebugData.grown.toLong
        val laySpeed = dut.vertices(0).layersDebugData.speed.toLong
        val layVirtual = dut.vertices(0).layersDebugData.isVirtual.toBoolean

        assert(layNode == 0, s"layers[0] node should be 0, got $layNode")
        assert(layRoot == 0, s"layers[0] root should be 0, got $layRoot")
        assert(layDefect, s"layers[0] isDefect should be true")
        assert(layGrown == 1, s"layers[0] grown should be 1, got $layGrown")
        assert(laySpeed == Speed.Grow, s"layers[0] speed should be Grow, got $laySpeed")
        assert(!layVirtual, s"layers[0] isVirtual should be false")

        // Also verify v3 (another elastic vertex, was idle) — layers[0] should hold its reset state
        dut.vertices(3).layersDebugAddr #= 0
        sleep(1)
        val lay3Node = dut.vertices(3).layersDebugData.node.toLong
        val lay3Grown = dut.vertices(3).layersDebugData.grown.toLong
        val lay3Defect = dut.vertices(3).layersDebugData.isDefect.toBoolean

        assert(lay3Node == config.IndexNone, s"v3 layers[0] node should be IndexNone, got $lay3Node")
        assert(lay3Grown == 0, s"v3 layers[0] grown should be 0, got $lay3Grown")
        assert(!lay3Defect, s"v3 layers[0] isDefect should be false")

        println("Layers commit test passed")

        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

  test("basic shift - states move down one layer") {
    // After Reset + AddDefect(v24, 0) + LoadDefectsExternal(3) + Grow(2), issue ArchiveElasticSlice.
    // Verify each layer received its donor's state:
    //   v16 <- v24's old state (node=0, grown=2, isDefect=true)
    //   v8  <- v16's old state (reset/virtual)
    //   v0  <- v8's old state  (reset/virtual)
    //   v24 <- reset (top layer, mode 2)
    val config = DualConfig(filename = "./resources/graphs/example_phenomenological_rotated_d3.json", minimizeBits = false)
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq()
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 1
    config.sanityCheck()
    Config.sim
      .compile({ val dut = DistributedDual(config, ioConfig); dut.simMakePublicSnapshot(); dut })
      .doSim("testBasicShift") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        dut.simExecute(ioConfig.instructionSpec.generateReset())
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(24, 0))
        dut.simExecute(ioConfig.instructionSpec.generateLoadDefectsExternal(3))
        dut.simExecute(ioConfig.instructionSpec.generateGrow(2))

        sleep(1)
        // Pre-shift sanity
        assert(dut.vertices(24).register.node.toLong == 0)
        assert(dut.vertices(24).register.grown.toLong == 2)
        assert(dut.vertices(24).register.isDefect.toBoolean)
        assert(dut.vertices(16).register.node.toLong == config.IndexNone)
        assert(dut.vertices(8).register.node.toLong == config.IndexNone)
        assert(dut.vertices(0).register.node.toLong == config.IndexNone)

        dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())
        sleep(1)

        // v16 got v24's state
        assert(dut.vertices(16).register.node.toLong == 0, "v16 should have v24's node=0")
        assert(dut.vertices(16).register.grown.toLong == 2, "v16 should have v24's grown=2")
        assert(dut.vertices(16).register.isDefect.toBoolean, "v16 should have v24's isDefect")
        assert(!dut.vertices(16).register.isVirtual.toBoolean, "v16 should inherit v24's isVirtual=false")

        // v8 got v16's old state (was virtual, no node)
        assert(dut.vertices(8).register.node.toLong == config.IndexNone, "v8 should have v16's old reset node")
        assert(dut.vertices(8).register.grown.toLong == 0, "v8 should have v16's old grown=0")

        // v0 got v8's old state (was virtual, no node)
        assert(dut.vertices(0).register.node.toLong == config.IndexNone, "v0 should have v8's old reset node")
        assert(dut.vertices(0).register.grown.toLong == 0, "v0 should have v8's old grown=0")

        // v24 was reset (mode 2)
        assert(dut.vertices(24).register.node.toLong == config.IndexNone, "v24 should be reset")
        assert(dut.vertices(24).register.grown.toLong == 0, "v24 grown should be 0 after reset")
        assert(!dut.vertices(24).register.isDefect.toBoolean, "v24 isDefect should be false after reset")

        // v0's layers should hold v0's pre-shift state (idle/reset)
        dut.vertices(0).layersDebugAddr #= 0
        sleep(1)
        assert(dut.vertices(0).layersDebugData.node.toLong == config.IndexNone, "v0 layers[0] should hold reset node")
        assert(dut.vertices(0).layersDebugData.grown.toLong == 0, "v0 layers[0] should hold grown=0")

        println("Basic shift test passed")
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

  test("holdForArchive prevents propagation during shift") {
    // Setup: make v12 a defect and grow until the edge v12-v4 is tight, so v4
    // would normally propagate v12's node via VertexPostUpdateState.
    // Then issue ArchiveElasticSlice. During the archive cycle, holdForArchive is true,
    // so v4 should NOT propagate — it should receive its donor v12's state via the shift
    // instead.
    //
    // Edge v4-v12 is edge 13 with weight=2. After Grow(1), both sides grow by 1.
    // v12 has grown=1 (speed=Grow from AddDefect). v4 has grown=0 (no node, speed=Stay).
    // Edge remaining = 2 - 1 - 0 = 1, not tight.
    // After Grow(2): v12 grown=2. Edge remaining = 2 - 2 - 0 = 0, tight.
    // Now v4 would normally propagate v12's node. But during ArchiveElasticSlice, holdForArchive
    // blocks this — v4's state after the archive should be its donor's (v12's pre-shift) state,
    // not a propagated state.
    val config = DualConfig(filename = "./resources/graphs/example_phenomenological_rotated_d3.json", minimizeBits = false)
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq()
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 1
    config.sanityCheck()
    Config.sim
      .compile({ val dut = DistributedDual(config, ioConfig); dut.simMakePublicSnapshot(); dut })
      .doSim("testHoldForArchive") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        dut.simExecute(ioConfig.instructionSpec.generateReset())
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(12, 0))
        dut.simExecute(ioConfig.instructionSpec.generateLoadDefectsExternal(1))
        dut.simExecute(ioConfig.instructionSpec.generateGrow(2))

        // Issue FindObstacle to trigger a second pipeline pass where v4 sees
        // v12's updated grown=2, making edge v4-v12 tight and enabling propagation.
        dut.simExecute(ioConfig.instructionSpec.generateFindObstacle())

        sleep(1)
        // v12 should have node=0, grown=2
        assert(dut.vertices(12).register.node.toLong == 0, "v12 node should be 0")
        assert(dut.vertices(12).register.grown.toLong == 2, "v12 grown should be 2")
        // v4 should have propagated node from v12 via tight edge after FindObstacle
        assert(dut.vertices(4).register.node.toLong == 0, "v4 should have propagated node=0 from v12")

        // Now issue ArchiveElasticSlice. During this cycle, holdForArchive prevents propagation.
        // v4 (mode=1, donor=v12) gets v12's pre-shift state via the shift chain.
        // v12's pre-shift state: node=0, grown=2, isDefect=true, speed=Grow
        dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())
        sleep(1)

        // v4 should now hold v12's pre-shift state (from the shift), NOT a propagated result
        assert(dut.vertices(4).register.node.toLong == 0, "v4 should hold v12's shifted node=0")
        assert(dut.vertices(4).register.grown.toLong == 2, "v4 should hold v12's shifted grown=2")
        assert(dut.vertices(4).register.isDefect.toBoolean, "v4 should hold v12's shifted isDefect=true")

        // v12 (mode=1, donor=v20) should hold v20's old state (reset)
        assert(dut.vertices(12).register.node.toLong == config.IndexNone, "v12 should hold v20's reset node")
        assert(dut.vertices(12).register.grown.toLong == 0, "v12 should hold v20's reset grown=0")

        println("holdForArchive test passed")
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

  test("non-column vertices unaffected by ArchiveElasticSlice") {
    // Virtual vertices not in any layer column (v1, v2, v5, v6, v9, v10, v13, v14, etc.)
    // should have archShiftMode=0 and be completely unchanged by ArchiveElasticSlice.
    val config = DualConfig(filename = "./resources/graphs/example_phenomenological_rotated_d3.json", minimizeBits = false)
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq()
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 1
    config.sanityCheck()
    Config.sim
      .compile({ val dut = DistributedDual(config, ioConfig); dut.simMakePublicSnapshot(); dut })
      .doSim("testNonColumnUnaffected") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        dut.simExecute(ioConfig.instructionSpec.generateReset())

        // Capture state of non-column vertices before archive
        sleep(1)
        val nonColumnVertices = Seq(1, 2, 5, 6, 9, 10, 13, 14, 17, 18, 21, 22, 25, 26, 29, 30)
        val beforeStates = nonColumnVertices.map { vi =>
          (vi,
           dut.vertices(vi).register.node.toLong,
           dut.vertices(vi).register.root.toLong,
           dut.vertices(vi).register.grown.toLong,
           dut.vertices(vi).register.isVirtual.toBoolean,
           dut.vertices(vi).register.isDefect.toBoolean,
           dut.vertices(vi).register.speed.toLong)
        }

        dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())
        sleep(1)

        // Verify all non-column vertices are unchanged
        for ((vi, bNode, bRoot, bGrown, bVirtual, bDefect, bSpeed) <- beforeStates) {
          assert(dut.vertices(vi).register.node.toLong == bNode, s"v$vi node changed")
          assert(dut.vertices(vi).register.root.toLong == bRoot, s"v$vi root changed")
          assert(dut.vertices(vi).register.grown.toLong == bGrown, s"v$vi grown changed")
          assert(dut.vertices(vi).register.isVirtual.toBoolean == bVirtual, s"v$vi isVirtual changed")
          assert(dut.vertices(vi).register.isDefect.toBoolean == bDefect, s"v$vi isDefect changed")
          assert(dut.vertices(vi).register.speed.toLong == bSpeed, s"v$vi speed changed")
        }

        println("Non-column unaffected test passed")
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

  test("shift with context switching") {
    // With contextDepth=2, ArchiveElasticSlice on context 0 should not affect context 1.
    val config = DualConfig(filename = "./resources/graphs/example_phenomenological_rotated_d3.json", minimizeBits = false)
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq()
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 2
    config.sanityCheck()
    Config.sim
      .compile({ val dut = DistributedDual(config, ioConfig); dut.simMakePublicSnapshot(); dut })
      .doSim("testContextShift") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        def simExecuteCtx(instruction: Long, ctx: Int) = {
          dut.io.message.valid #= true
          dut.io.message.instruction #= instruction
          dut.io.message.contextId #= ctx
          dut.clockDomain.waitSampling()
          dut.io.message.valid #= false
          for (_ <- 0 until config.readLatency - 1) { dut.clockDomain.waitSampling() }
          sleep(1)
        }

        // Reset both contexts
        simExecuteCtx(ioConfig.instructionSpec.generateReset(), 0)
        simExecuteCtx(ioConfig.instructionSpec.generateReset(), 1)

        // Context 0: AddDefect(v24, 0) + grow
        simExecuteCtx(ioConfig.instructionSpec.generateAddDefect(24, 0), 0)
        simExecuteCtx(ioConfig.instructionSpec.generateLoadDefectsExternal(3), 0)
        simExecuteCtx(ioConfig.instructionSpec.generateGrow(1), 0)

        // Context 1: AddDefect(v24, 1) + grow more
        simExecuteCtx(ioConfig.instructionSpec.generateAddDefect(24, 1), 1)
        simExecuteCtx(ioConfig.instructionSpec.generateLoadDefectsExternal(3), 1)
        simExecuteCtx(ioConfig.instructionSpec.generateGrow(2), 1)

        // Archive context 0 only
        simExecuteCtx(ioConfig.instructionSpec.generateArchiveElasticSlice(), 0)

        // Fetch context 0 state (need a FindObstacle or similar to trigger a read from ram)
        // Actually, we can read the ram via readAsync in the existing shift logic.
        // But registers only hold the last-written context. We need to trigger a fetch for each context.
        // Issue a no-op FindObstacle to load context 1 into the pipeline, then check registers.
        simExecuteCtx(ioConfig.instructionSpec.generateFindObstacle(), 1)

        // After fetching context 1: v24 should still have node=1, grown=2 (context 1 untouched)
        // In multi-context mode, read from pipeline output (stageOutputs) instead of register
        assert(dut.vertices(24).io.stageOutputs.updateGet3.state.node.toLong == 1, "ctx1 v24 node should be 1 (untouched)")
        assert(dut.vertices(24).io.stageOutputs.updateGet3.state.grown.toLong == 2, "ctx1 v24 grown should be 2 (untouched)")

        // Now fetch context 0 to verify the shift happened
        simExecuteCtx(ioConfig.instructionSpec.generateFindObstacle(), 0)

        // Context 0: v24 should be reset (mode 2)
        assert(dut.vertices(24).io.stageOutputs.updateGet3.state.node.toLong == config.IndexNone, "ctx0 v24 should be reset after archive")
        assert(dut.vertices(24).io.stageOutputs.updateGet3.state.grown.toLong == 0, "ctx0 v24 grown should be 0 after archive")
        // Context 0: v16 should hold v24's old ctx0 state (node=0, grown=1)
        assert(dut.vertices(16).io.stageOutputs.updateGet3.state.node.toLong == 0, "ctx0 v16 should have v24's old node=0")
        assert(dut.vertices(16).io.stageOutputs.updateGet3.state.grown.toLong == 1, "ctx0 v16 should have v24's old grown=1")

        // Now archive context 1
        simExecuteCtx(ioConfig.instructionSpec.generateArchiveElasticSlice(), 1)
        simExecuteCtx(ioConfig.instructionSpec.generateFindObstacle(), 1)

        // Context 1: v16 should hold v24's old ctx1 state (node=1, grown=2)
        assert(dut.vertices(16).io.stageOutputs.updateGet3.state.node.toLong == 1, "ctx1 v16 should have v24's old node=1")
        assert(dut.vertices(16).io.stageOutputs.updateGet3.state.grown.toLong == 2, "ctx1 v16 should have v24's old grown=2")

        println("Context switching shift test passed")
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

  test("shift preserves spatial edge tightness") {
    // Two adjacent top-layer defects v24 and v27. After growing, edge v24-v27 (edge 52, weight=2)
    // becomes tight. After ArchiveElasticSlice, the states shift to v16 and v19.
    // Edge v16-v19 (edge 35, weight=2) should detect a conflict between the two shifted clusters.
    val config = DualConfig(filename = "./resources/graphs/example_phenomenological_rotated_d3.json", minimizeBits = false)
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq()
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 1
    config.sanityCheck()
    Config.sim
      .compile({ val dut = DistributedDual(config, ioConfig); dut.simMakePublicSnapshot(); dut })
      .doSim("testShiftTightness") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        dut.simExecute(ioConfig.instructionSpec.generateReset())
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(24, 0))
        dut.simExecute(ioConfig.instructionSpec.generateAddDefect(27, 1))
        dut.simExecute(ioConfig.instructionSpec.generateLoadDefectsExternal(3))

        // Grow by 1: both grow. Edge 52 (v24-v27, w=2): remaining = 2-1-1 = 0 -> tight -> conflict
        val (_, conflict, grown) = dut.simFindObstacle(1)
        assert(conflict.valid, "should have conflict between v24 and v27")
        assert(grown == 1, s"should grow by 1, got $grown")

        // Shift: v24->v16, v27->v19
        dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())
        sleep(1)

        // v16 should have node=0, grown=1; v19 should have node=1, grown=1
        assert(dut.vertices(16).register.node.toLong == 0, "v16 should have node=0 from v24")
        assert(dut.vertices(16).register.grown.toLong == 1, "v16 should have grown=1 from v24")
        assert(dut.vertices(19).register.node.toLong == 1, "v19 should have node=1 from v27")
        assert(dut.vertices(19).register.grown.toLong == 1, "v19 should have grown=1 from v27")

        // FindObstacle should detect the conflict on edge v16-v19 (edge 35, w=2): 1+1=2 >= 2
        val (maxGrowable2, conflict2) = dut.simExecute(ioConfig.instructionSpec.generateFindObstacle())
        assert(conflict2.valid, "should detect conflict between shifted clusters on v16-v19")
        // The conflict should involve nodes 0 and 1
        val nodes = Set(conflict2.node1, conflict2.node2)
        assert(nodes.contains(0) && nodes.contains(1),
          s"conflict should involve nodes 0 and 1, got ${conflict2.node1} and ${conflict2.node2}")

        println("Shift tightness test passed")
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

  test("multiple archives fill layers correctly") {
    // Issue 3 rounds of AddDefect + Grow + ArchiveElasticSlice with different node IDs and
    // growth amounts. Since contextDepth=1 there is only 1 layers slot, so each archive
    // overwrites the previous. Verify via layersDebugData that layers[0] always holds the
    // most recently archived state.
    val config = DualConfig(filename = "./resources/graphs/example_phenomenological_rotated_d3.json", minimizeBits = false)
    config.supportLayerFusion = true
    val ioConfig = DualConfig()
    config.graph.offloading = Seq()
    config.fitGraph(minimizeBits = false)
    config.contextDepth = 1
    config.sanityCheck()
    Config.sim
      .compile({ val dut = DistributedDual(config, ioConfig); dut.simMakePublicSnapshot(); dut })
      .doSim("testMultipleArchives") { dut =>
        dut.io.message.valid #= false
        dut.clockDomain.forkStimulus(period = 10)
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }

        dut.simExecute(ioConfig.instructionSpec.generateReset())

        for (round <- 0 until 3) {
          // Each round: put defect on v0 with node=round, grow by (round+1)
          dut.simExecute(ioConfig.instructionSpec.generateAddDefect(0, round))
          dut.simExecute(ioConfig.instructionSpec.generateLoadDefectsExternal(0))
          dut.simExecute(ioConfig.instructionSpec.generateGrow(round + 1))

          sleep(1)
          val preNode = dut.vertices(0).register.node.toLong
          val preGrown = dut.vertices(0).register.grown.toLong
          assert(preNode == round, s"round $round: v0 node should be $round before archive, got $preNode")
          assert(preGrown == round + 1, s"round $round: v0 grown should be ${round + 1} before archive, got $preGrown")

          dut.simExecute(ioConfig.instructionSpec.generateArchiveElasticSlice())
          sleep(1)

          // layers[0] should hold this round's pre-shift state
          dut.vertices(0).layersDebugAddr #= 0
          sleep(1)
          val layNode = dut.vertices(0).layersDebugData.node.toLong
          val layGrown = dut.vertices(0).layersDebugData.grown.toLong
          assert(layNode == round, s"round $round: layers[0] node should be $round, got $layNode")
          assert(layGrown == round + 1, s"round $round: layers[0] grown should be ${round + 1}, got $layGrown")

          // v0's register should now hold v8's state (reset each time since v8 keeps getting
          // reset states shifted down from above)
          assert(dut.vertices(0).register.node.toLong == config.IndexNone,
            s"round $round: v0 register should be reset after archive")
        }

        println("Multiple archives test passed")
        for (_ <- 0 to 10) { dut.clockDomain.waitSampling() }
      }
  }

}

// sbt "runMain microblossom.modules.DistributedDualTestDebug1"
object DistributedDualTestDebug1 extends App {
  // gtkwave simWorkspace/DistributedDualTest/testB.fst
  val config = DualConfig(filename = "./resources/graphs/example_code_capacity_planar_d3.json", minimizeBits = false)
  val ioConfig = DualConfig()
  config.graph.offloading = Seq() // remove all offloaders
  config.fitGraph(minimizeBits = false)
  config.sanityCheck()
  Config.sim
    .compile({
      val dut = DistributedDual(config, ioConfig)
      dut.simMakePublicSnapshot()
      dut
    })
    .doSim("testB") { dut =>
      dut.io.message.valid #= false
      dut.clockDomain.forkStimulus(period = 10)

      for (idx <- 0 to 10) { dut.clockDomain.waitSampling() }

      dut.simExecute(ioConfig.instructionSpec.generateReset())
      dut.simExecute(ioConfig.instructionSpec.generateAddDefect(0, 0))
      dut.simExecute(ioConfig.instructionSpec.generateAddDefect(4, 1))
      dut.simExecute(ioConfig.instructionSpec.generateAddDefect(8, 2))

      val (_, conflict, grown) = dut.simFindObstacle(1000)
      assert(grown == 1)
      assert(conflict.valid == true)
      println(conflict.node1, conflict.node2, conflict.touch1, conflict.touch2)

      println(dut.simSnapshot().noSpacesSortKeys)

      dut.simExecute(ioConfig.instructionSpec.generateSetBlossom(0, 3000))
      dut.simExecute(ioConfig.instructionSpec.generateSetBlossom(1, 3000))
      dut.simExecute(ioConfig.instructionSpec.generateSetBlossom(2, 3000))
      dut.simExecute(ioConfig.instructionSpec.generateSetSpeed(3, Speed.Shrink))

      for (idx <- 0 to 10) { dut.clockDomain.waitSampling() }

    }
}

object Local {

  def dualConfig(name: String, removeWeight: Boolean = false): DualConfig = {
    val config = DualConfig(filename = s"./resources/graphs/example_$name.json")
    if (removeWeight) {
      for (edgeIndex <- 0 until config.edgeNum) {
        config.graph.weighted_edges(edgeIndex).w = 2
      }
      config.fitGraph()
      require(config.weightBits == 2)
    }
    config
  }
}

// sbt 'runMain microblossom.modules.DistributedDualEstimation'
object DistributedDualEstimation extends App {
  val configurations = List(
    // synth: 387, impl: 310 Slice LUTs (0.14% on ZCU106), or 407 CLB LUTs (0.05% on VMK180)
    (Local.dualConfig("code_capacity_d5"), "code capacity repetition d=5"),
    // synth: 1589, impl: 1309 Slice LUTs (0.6% on ZCU106), or 1719 CLB LUTs (0.19% on VMK180)
    (Local.dualConfig("code_capacity_rotated_d5"), "code capacity rotated d=5"),
    // synth: 15470, impl: 13186 Slice LUTs (6.03% on ZCU106)
    (Local.dualConfig("phenomenological_rotated_d5"), "phenomenological d=5"),
    // synth: 31637, impl: 25798 Slice LUTs (11.80% on ZCU106)
    (Local.dualConfig("circuit_level_d5", true), "circuit-level d=5 (unweighted)"),
    // synth: 41523, impl: 34045 Slice LUTs (15.57% on ZCU106)
    (Local.dualConfig("circuit_level_d5"), "circuit-level d=5"),
    // synth: 299282, impl:
    (Local.dualConfig("circuit_level_d9"), "circuit-level d=9")
  )
  for ((config, name) <- configurations) {
    // val reports = Vivado.report(DistributedDual(config, config))
    val reports = Vivado.report(DistributedDual(config, config), useImpl = true)
    println(s"$name:")
    // reports.resource.primitivesTable.print()
    reports.resource.netlistLogicTable.print()
  }
}

// sbt 'runMain microblossom.modules.DistributedDualPhenomenologicalEstimation'
object DistributedDualPhenomenologicalEstimation extends App {
  // post-implementation estimations on VMK180
  // d=3: 3873 LUTs (0.43%), 670 Registers (0.04%)
  // d=5: 17293 LUTs (1.92%), 2474 Registers (0.14%)
  // d=7: 48709 LUTs (5.41%), 6233 Registers (0.35%)
  // d=9: 106133 LUTs (11.79%), 13170 Registers (0.73%)
  // d=11: 205457 LUTs (22.83%), 24621 Registers (1.37%)
  // d=13: 354066 LUTs (39.35%), 41790 Registers (2.32%)
  // d=15: 529711 LUTs (58.87%), 62267 Registers (3.46%)
  // d=17: 799536 LUTs (88.85%), 94875 Registers (5.27%)
  for (d <- List(3, 5, 7, 9, 11, 13, 15, 17)) {
    val config = Local.dualConfig(s"phenomenological_rotated_d$d")
    val reports = Vivado.report(DistributedDual(config, config), useImpl = true)
    println(s"phenomenological d = $d:")
    reports.resource.netlistLogicTable.print()
  }
}

// sbt 'runMain microblossom.modules.DistributedDualCircuitLevelUnweightedEstimation'
object DistributedDualCircuitLevelUnweightedEstimation extends App {
  // post-implementation estimations on VMK180
  // d=3: 6540 LUTs (0.73%), 1064 Registers (0.06%)
  // d=5: 39848 LUTs (4.43%), 4558 Registers (0.25%)
  // d=7: 128923 LUTs (14.33%), 12387 Registers (0.69%)
  // d=9: 307017 LUTs (34.12%), 26856 Registers (1.49%)
  // d=11: 609352 LUTs (67.72%), 50385 Registers (2.80%)
  for (d <- List(3, 5, 7, 9, 11)) {
    val config = Local.dualConfig(s"circuit_level_d$d", removeWeight = true)
    val reports = Vivado.report(DistributedDual(config, config), useImpl = true)
    println(s"circuit-level unweighted d = $d:")
    reports.resource.netlistLogicTable.print()
  }
}

// sbt 'runMain microblossom.modules.DistributedDualCircuitLevelEstimation'
object DistributedDualCircuitLevelEstimation extends App {
  // post-implementation estimations on VMK180
  // d=3: 7575 LUTs (0.84%), 1110 Registers (0.06%)
  // d=5: 46483 LUTs (5.17%), 4684 Registers (0.26%)
  // d=7: 146994 LUTs (16.34%), 12576 Registers (0.70%)
  // d=9: 342254 LUTs (38.03%), 27151 Registers (1.51%)
  // d=11: 679519 LUTs (75.52%), 50907 Registers (2.83%)
  for (d <- List(3, 5, 7, 9, 11)) {
    val config = Local.dualConfig(s"circuit_level_d$d")
    val reports = Vivado.report(DistributedDual(config, config), useImpl = true)
    println(s"circuit-level d = $d:")
    reports.resource.netlistLogicTable.print()
  }
}

// sbt 'runMain microblossom.modules.DistributedDualContextDepthEstimation'
object DistributedDualContextDepthEstimation extends App {
  // post-implementation estimations on VMK180
  // depth=1: 342254 LUTs (38.03%), 27151 Registers (1.51%)
  // depth=2: 429404 LUTs (47.72%), 52555 Registers (2.92%)
  // depth=4: 433277 LUTs (48.15%), 56941 Registers (3.16%)
  // depth=8: 436583 LUTs (48.52%), 61789 Registers (3.43%)
  // depth=16: 435172 LUTs (48.36%), 66428 Registers (3.69%)
  // depth=32: 436308 LUTs (48.49%), 71110 Registers (3.95%)
  // depth=64: 457438 LUTs (50.84%), 75760 Registers (4.21%)
  // depth=1024: seg fault, potentially due to insufficient memory
  val d = 9
  for (contextDepth <- List(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048)) {
    val config = Local.dualConfig(s"circuit_level_d$d")
    config.contextDepth = contextDepth
    val reports = Vivado.report(DistributedDual(config, config), useImpl = true)
    println(s"contextDepth = $contextDepth:")
    reports.resource.netlistLogicTable.print()
  }
}

// sbt "runMain microblossom.modules.DistributedDualExamples"
object DistributedDualExamples extends App {
  for (d <- Seq(3, 5, 7)) {
    val config =
      DualConfig(filename = "./resources/graphs/example_code_capacity_planar_d%d.json".format(d), minimizeBits = true)
    Config.spinal("gen/example_code_capacity_planar_d%d".format(d)).generateVerilog(DistributedDual(config, config))
  }
  for (d <- Seq(3, 5, 7)) {
    val config =
      DualConfig(filename = "./resources/graphs/example_code_capacity_rotated_d%d.json".format(d), minimizeBits = true)
    Config.spinal("gen/example_code_capacity_rotated_d%d".format(d)).generateVerilog(DistributedDual(config, config))
  }
  for (d <- Seq(3, 5, 7, 9, 11)) {
    val config =
      DualConfig(
        filename = "./resources/graphs/example_phenomenological_rotated_d%d.json".format(d),
        minimizeBits = true
      )
    config.broadcastDelay = 2
    config.convergecastDelay = 4
    Config.spinal("gen/example_phenomenological_rotated_d%d".format(d)).generateVerilog(DistributedDual(config, config))
  }
}

// Note: to further increase the memory limit to instantiate even larger instances, see `javaOptions` in `build.sbt`
// sbt "runMain microblossom.modules.DistributedDualLargeInstance"
object DistributedDualLargeInstance extends App {
  val config = Local.dualConfig(s"circuit_level_d13")
  Config.spinal().generateVerilog(DistributedDual(config, config))
}
