package microblossom

import spinal.core._
import util._
import io.circe.parser.decode
import collection.mutable.ArrayBuffer
import org.scalatest.funsuite.AnyFunSuite

object DualConfig {
  def version = Integer.parseInt("24" + "01" + "23" + "c0", 16) // year - month - date - 'c'revision
}

case class DualConfig(
    var vertexBits: Int = 15,
    var weightBits: Int = 26,
    var broadcastDelay: Int = 0,
    var convergecastDelay: Int = 1, // the write or register update takes 1 clock cycle, so delay the output by 1
    // Configured pipeline stages for the convergecast reduction trees (maxGrowable + conflict).
    // Each stage tries to insert one `RegNext` layer to cut the route-dominated critical path.
    // The trees produced by `inferred_from_positions` are NOT balanced, so the actual added
    // latency is computed per-tree (`convergecastActualPipelineDelay`); paths inside the tree
    // are aligned with extra `RegNext`s so every leaf-to-root path has the same total delay.
    var convergecastPipelineStages: Int = 0,
    var instructionBufferDepth: Int = 4, // buffer write instructions for higher throughput, must be a power of 2
    var contextDepth: Int = 1, // how many different contexts are supported
    var conflictChannels: Int = 1, // how many conflicts are collected at once in parallel
    var hardCodeWeights: Boolean = true, // hard-code the edge weights to simplify logic
    // optional features
    var supportAddDefectVertex: Boolean = true,
    var supportOffloading: Boolean = false,
    var supportLayerFusion: Boolean = false,
    var supportLoadStallEmulator: Boolean = false,
    // load graph either from parameter or from file
    var graph: SingleGraph = null,
    val filename: String = null,
    val minimizeBits: Boolean = true,
    var injectRegisters: Seq[String] = List(),
    /** Depth of the elastic `layers` BRAM per vertex. Sized to the problem; never wraps. */
    var archiveDepth: Int = 1
) {
  assert(isPow2(instructionBufferDepth) & instructionBufferDepth >= 2)
  if (supportLoadStallEmulator) {
    assert(supportLayerFusion)
  }

  /**
    * When `supportLayerFusion` is on, same-L0 fusion vertical edges use **L0 live** grown/shadow on the upper endpoint and
    * `RegNext(L0 archived …)` on the lower — parallel to archive-slice `k` vs `k−1` pairing (see `Edge.archivedGrownAt`).
    * Only affects edges where `isFusionEdgeSameL0`.
    */
  def fusionElasticTightUsesLiveVsArchived0: Boolean = supportLayerFusion

  def vertexNum = graph.vertex_num.toInt
  def edgeNum = graph.weighted_edges.length.toInt
  def offloaderNum = activeOffloading.length.toInt
  def instructionSpec = InstructionSpec(this)
  def contextBits = log2Up(contextDepth)
  def instructionBufferBits = log2Up(2 * instructionBufferDepth + 2) // the dual module may be processing an instruction
  def IndexNone = (1 << vertexBits) - 1
  def LengthNone = (1 << weightBits) - 1
  def supportContextSwitching = contextBits > 0
  def executeLatency = { // from sending the command to the time it's safe to write to the same context again
    // when context switching, 2 cycles delay due to memory fetch and write
    val contextDelay = 2 * (contextDepth != 1).toInt
    injectRegisters.length + contextDelay
  }
  /** Actual pipeline delay added at the convergecast tree roots, taken as the larger of the
    * vertex_edge and edge tree (the smaller is padded to match inside `DistributedDual`). */
  def convergecastActualPipelineDelay: Int = {
    if (graph == null || convergecastPipelineStages <= 0) 0
    else {
      val mg = ConvergecastPipelining.rootDelay(graph.vertex_edge_binary_tree.nodes, convergecastPipelineStages)
      val cf = ConvergecastPipelining.rootDelay(graph.edge_binary_tree.nodes, convergecastPipelineStages)
      scala.math.max(mg, cf)
    }
  }
  def readLatency = { // from sending the command to receiving the obstacle
    broadcastDelay + convergecastDelay + convergecastActualPipelineDelay + executeLatency
  }
  def layerFusion = {
    graph.layer_fusion match {
      case Some(layer_fusion) => layer_fusion
      case None               => LayerFusion(0, Seq(), Map(), Map(), Map())
    }
  }
  def numLayers = layerFusion.num_layers
  /** First time-slice (`layers(0)`) vertices own elastic `layers` BRAM (see `Vertex.elastic`). */
  def vertexHasElasticLayers(vertexIndex: Int): Boolean =
    numLayers > 0 && layerFusion.layers.nonEmpty &&
      layerFusion.layers(0).exists(_.toInt == vertexIndex)
  def layerIdBits = log2Up(numLayers)
  def parityReporters = {
    graph.parity_reporters match {
      case Some(parity_reporters) => parity_reporters.reporters
      case None                   => Seq()
    }
  }
  def parityReportersNum = parityReporters.length.toInt

  private val virtualVertices = collection.mutable.Set[Int]()
  private val incidentEdges = collection.mutable.Map[Int, ArrayBuffer[Int]]() // vertexIndex -> Seq[edgeIndex]
  private val incidentOffloaders = collection.mutable.Map[Int, ArrayBuffer[Int]]() // vertexIndex -> Seq[offloaderIndex]
  val activeOffloading = ArrayBuffer[Offloading]()
  val edgeConditionedVertex = collection.mutable.Map[Int, Int]()
  val vertexLayerId = collection.mutable.Map[Int, Int]()

  /// Per vertex: 0 = `ArchiveElasticSlice` does not update live storage; 1 = copy donor; 2 = load reset.
  private var archiveElasticLayerShiftMode: Array[Int] = Array.empty[Int]
  /// When mode==1, donor vertex index (strictly higher layer).
  /// look up table for whether to copy from donor, reset, or do nothing (0)
  private var archiveElasticLayerShiftDonor: Array[Int] = Array.empty[Int]
  /// For each vertex, the layer-0 vertex whose donor chain reaches it (inverse of shift donor).
  private var layer0Counterpart_ : Array[Int] = Array.empty[Int]

  def archiveElasticLayerShiftModeOf(vertexIndex: Int): Int = {
    if (archiveElasticLayerShiftMode.isEmpty) 0
    else archiveElasticLayerShiftMode(vertexIndex)
  }
  def archiveElasticLayerShiftDonorOf(vertexIndex: Int): Int = {
    if (archiveElasticLayerShiftDonor.isEmpty) 0
    else archiveElasticLayerShiftDonor(vertexIndex)
  }
  def archiveAddressBits: Int = log2Up(archiveDepth max 2)
  /** Layer-0 vertex whose donor chain reaches `vertexIndex`. Identity for layer-0 vertices. */
  def layer0CounterpartOf(vertexIndex: Int): Int = {
    if (layer0Counterpart_.isEmpty) vertexIndex
    else layer0Counterpart_(vertexIndex)
  }
  /** Layer of an edge, derived from its endpoints' vertexLayerId. Fusion edge → lower layer. None for non-layer edges. */
  def edgeLayerOf(edgeIndex: Int): Option[Int] = {
    val (l, r) = incidentVerticesOf(edgeIndex)
    (vertexLayerId.get(l), vertexLayerId.get(r)) match {
      case (Some(ll), Some(lr)) => Some(ll min lr)
      case (Some(ll), None)     => Some(ll)
      case (None, Some(lr))     => Some(lr)
      case (None, None)         => None
    }
  }
  /** BRAM indices that an edge at layer `edgeLayer` should check.
    * The elastic shift brings all data to layer 0 before committing, so the BRAM layout is sequential
    * (addresses 0, 1, 2, ...). Every entry contains layer-0 data and must be checked by all edges. */
  def archiveScanAddressesOf(edgeLayer: Int): Seq[Int] =
    (0 until archiveDepth)
  /** True if this is a fusion edge whose both endpoints map to the same layer-0 counterpart vertex. */
  def isFusionEdgeSameL0(edgeIndex: Int): Boolean = {
    val (l, r) = incidentVerticesOf(edgeIndex)
    val ll0 = layer0CounterpartOf(l)
    val rl0 = layer0CounterpartOf(r)
    ll0 == rl0 && vertexHasElasticLayers(ll0)
  }
  /** For a fusion edge with same L0 counterpart, which endpoint is the upper layer (needs scanIndex+1). */
  def fusionEdgeUpperVertex(edgeIndex: Int): Int = {
    val (l, r) = incidentVerticesOf(edgeIndex)
    val ll = vertexLayerId.getOrElse(l, -1)
    val rl = vertexLayerId.getOrElse(r, -1)
    if (ll > rl) l else r
  }

  if (filename != null) {
    val source = scala.io.Source.fromFile(filename)
    val json_content =
      try source.getLines.mkString
      finally source.close()
    assert(graph == null, "cannot provide both graph and filename")
    graph = decode[SingleGraph](json_content) match {
      case Right(graph) => graph
      case Left(ex)     => throw ex
    }
    fitGraph(minimizeBits)
  } else if (graph != null) {
    fitGraph(minimizeBits)
  }

  // fit the bits to a specific decoding graph and construct connections
  def fitGraph(minimizeBits: Boolean = true): Unit = {
    // compute the minimum bits of vertices and nodes; note that there could be
    // as many as 2x nodes than the number of vertices, so it's necessary to have enough bits
    assert(vertexNum > 0)
    if (minimizeBits) {
      val max_node_num = vertexNum * 2
      vertexBits = log2Up(max_node_num)
      val max_weight = graph.weighted_edges.map(e => e.w).max
      assert(max_weight > 0)
      // weightBits = log2Up(max_weight.toInt * graph.weighted_edges.length)
      weightBits = log2Up(max_weight.toInt + 1) // weightBits could be smaller than grownBits
      assert(weightBits <= 26)
      if (weightBits > vertexBits * 2 - 4) {
        vertexBits = (weightBits + 5) / 2 // expand vertexBits so that the instruction can hold the maximum length
      }
      if (vertexBits < 5) {
        vertexBits = 5 // at least 5 bits to support all instructions
      }
    }
    // update virtual vertices
    virtualVertices.clear()
    for (vertexIndex <- graph.virtual_vertices) {
      virtualVertices += vertexIndex.toInt
    }
    // build vertex to neighbor edge mapping
    updateIncidentEdges()
    updateOffloading()
    updateArchiveElasticLayerShiftTable()
  }

  /** Build donor map: each vertex in layer L copies from same index in layer L+1; top layer resets.
    * Also builds the inverse `layer0Counterpart_` map.
    */
  def updateArchiveElasticLayerShiftTable(): Unit = {
    archiveElasticLayerShiftMode = Array.fill(vertexNum)(0)
    archiveElasticLayerShiftDonor = Array.fill(vertexNum)(0)
    layer0Counterpart_ = Array.tabulate(vertexNum)(identity) // default: self
    if (supportLayerFusion && numLayers >= 2) {
      val lf = layerFusion
      val nl = numLayers.toInt
      for (L <- 0 until nl - 1) {
        val below = lf.layers(L).map(_.toInt)
        val above = lf.layers(L + 1).map(_.toInt)
        assert(
          below.length == above.length,
          s"layer fusion: layers($L) and layers(${L + 1}) must have equal length for archive+layer-shift"
        )
        for (i <- below.indices) {
          archiveElasticLayerShiftMode(below(i)) = 1
          archiveElasticLayerShiftDonor(below(i)) = above(i)
        }
      }
      // ensure last layer is set to reset not shift
      for (v <- lf.layers(nl - 1)) {
        archiveElasticLayerShiftMode(v.toInt) = 2
      }
      // build layer-0 counterpart map: for each vertex in layer L>0,
      // follow the donor chain backward to find its layer-0 counterpart
      val layer0 = lf.layers(0).map(_.toInt)
      for (i <- layer0.indices) {
        // layer0(i) is the base; its donor chain goes layer0(i) ← layer1(i) ← layer2(i) ← ...
        var v = layer0(i)
        layer0Counterpart_(v) = layer0(i)
        for (L <- 1 until nl) {
          v = lf.layers(L)(i).toInt
          layer0Counterpart_(v) = layer0(i)
        }
      }
    }
  }

  def updateIncidentEdges() = {
    incidentEdges.clear()
    for ((edge, edgeIndex) <- graph.weighted_edges.zipWithIndex) {
      for (vertexIndex <- Seq(edge.l.toInt, edge.r.toInt)) {
        if (!incidentEdges.contains(vertexIndex)) {
          incidentEdges(vertexIndex) = ArrayBuffer()
        }
        incidentEdges(vertexIndex).append(edgeIndex)
      }
    }
  }
  def updateOffloading(): Unit = {
    incidentOffloaders.clear()
    activeOffloading.clear()
    edgeConditionedVertex.clear()
    vertexLayerId.clear()
    if (supportOffloading) {
      for (offloading <- graph.offloading) {
        activeOffloading.append(offloading)
      }
    }
    if (supportLayerFusion) {
      for ((edgeIndex, conditionedVertex) <- layerFusion.fusion_edges) {
        if (supportOffloading) {
          activeOffloading.append(Offloading(fm = Some(FusionMatch(edgeIndex, conditionedVertex))))
        }
        edgeConditionedVertex(edgeIndex.toInt) = conditionedVertex.toInt
      }
      for ((vertexIndex, layerId) <- layerFusion.vertex_layer_id) {
        vertexLayerId(vertexIndex.toInt) = layerId.toInt
      }
    }
    for ((offloader, offloaderIndex) <- activeOffloading.zipWithIndex) {
      for (vertexIndex <- offloaderNeighborVertexIndices(offloaderIndex)) {
        if (!incidentOffloaders.contains(vertexIndex)) {
          incidentOffloaders(vertexIndex) = ArrayBuffer()
        }
        incidentOffloaders(vertexIndex).append(offloaderIndex)
      }
    }
  }

  def numIncidentEdgeOf(vertexIndex: Int): Int = {
    return incidentEdgesOf(vertexIndex).length
  }
  def incidentEdgesOf(vertexIndex: Int): Seq[Int] = {
    return incidentEdges.getOrElse(vertexIndex, Seq())
  }
  def incidentOffloaderOf(vertexIndex: Int): Seq[Int] = {
    return incidentOffloaders.getOrElse(vertexIndex, Seq())
  }
  def numIncidentOffloaderOf(vertexIndex: Int): Int = {
    return incidentOffloaderOf(vertexIndex).length
  }
  def incidentVerticesOf(edgeIndex: Int): (Int, Int) = {
    val edge = graph.weighted_edges(edgeIndex)
    return (edge.l.toInt, edge.r.toInt)
  }
  def incidentVerticesPairsOf(edgeIndex: Int): Seq[Seq[Int]] = {
    val edge = graph.weighted_edges(edgeIndex)
    return Seq(
      Seq(edge.l.toInt, edge.r.toInt),
      Seq(edge.r.toInt, edge.l.toInt)
    )
  }
  def peerVertexOfEdge(edgeIndex: Int, vertexIndex: Int): Int = {
    val edge = graph.weighted_edges(edgeIndex)
    if (edge.l.toInt == vertexIndex) {
      return edge.r.toInt
    } else if (edge.r.toInt == vertexIndex) {
      return edge.l.toInt
    } else {
      throw new Exception(s"vertex $vertexIndex is not incident to edge $edgeIndex")
    }
  }
  def localIndexOfEdge(vertexIndex: Int, edgeIndex: Int): Int = {
    for ((localEdgeIndex, localIndex) <- incidentEdgesOf(vertexIndex).zipWithIndex) {
      if (localEdgeIndex == edgeIndex) {
        return localIndex
      }
    }
    throw new Exception("cannot find edge in the incident list of vertex")
  }
  def localIndexOfVertex(edgeIndex: Int, vertexIndex: Int): Int = {
    val edge = graph.weighted_edges(edgeIndex)
    if (edge.l == vertexIndex) {
      return 0
    }
    if (edge.r == vertexIndex) {
      return 1
    }
    throw new Exception("the edge does not connect the vertex")
  }
  def isVirtual(vertexIndex: Int): Boolean = {
    virtualVertices.contains(vertexIndex)
  }
  def grownBitsOf(vertexIndex: Int): Int = {
    log2Up(graph.vertex_max_growth(vertexIndex) + 1).max(weightBits)
  }
  def offloaderTypeOf(offloaderIndex: Int): String = {
    val offloader = activeOffloading(offloaderIndex)
    offloader.dm match {
      case Some(defectMatch) =>
        return "defect_match"
      case None =>
    }
    offloader.vm match {
      case Some(virtualMatch) =>
        return "virtual_match"
      case None =>
    }
    throw new Exception("unrecognized definition of offloader")
  }
  // (edgeIndex, neighborVertices, neighborEdges)
  def offloaderInformation(offloaderIndex: Int): (Int, Seq[Int], Seq[Int]) = {
    val offloader = activeOffloading(offloaderIndex)
    offloader.dm match {
      case Some(defectMatch) =>
        val edgeIndex = defectMatch.e.toInt
        val (left, right) = incidentVerticesOf(edgeIndex)
        return (edgeIndex, Seq(left, right), Seq())
      case None =>
    }
    offloader.vm match {
      case Some(virtualMatch) =>
        val edgeIndex = virtualMatch.e.toInt
        val edge = graph.weighted_edges(edgeIndex)
        val virtualVertex = virtualMatch.v.toInt
        val regularVertex = peerVertexOfEdge(edgeIndex, virtualVertex)
        val neighborEdges = incidentEdgesOf(regularVertex)
        return (
          edgeIndex,
          neighborEdges.map(ei => peerVertexOfEdge(ei, regularVertex)).filter(_ != virtualVertex) ++
            List(virtualVertex, regularVertex),
          neighborEdges.filter(_ != edgeIndex)
        )
      case None =>
    }
    offloader.fm match {
      case Some(fusionMatch) =>
        val edgeIndex = fusionMatch.e.toInt
        val conditionalVertex = fusionMatch.c.toInt
        val regularVertex = peerVertexOfEdge(edgeIndex, conditionalVertex)
        return (edgeIndex, Seq(conditionalVertex, regularVertex), Seq())
      case None =>
    }
    throw new Exception("unrecognized definition of offloader")
  }
  def offloaderEdgeIndex(offloaderIndex: Int): Int = {
    val (edgeIndex, neighborVertices, neighborEdges) = offloaderInformation(offloaderIndex)
    edgeIndex
  }
  def offloaderNeighborVertexIndices(offloaderIndex: Int): Seq[Int] = {
    val (edgeIndex, neighborVertices, neighborEdges) = offloaderInformation(offloaderIndex)
    neighborVertices
  }
  def offloaderNeighborEdgeIndices(offloaderIndex: Int): Seq[Int] = {
    val (edgeIndex, neighborVertices, neighborEdges) = offloaderInformation(offloaderIndex)
    neighborEdges
  }
  def numOffloaderNeighborOf(offloaderIndex: Int): Int = {
    offloaderNeighborVertexIndices(offloaderIndex).length
  }

  def sanityCheck(): Unit = {
    assert(vertexBits <= 15)
    assert(vertexBits > 0)
    assert(weightBits <= 30)
    assert(weightBits > 0)
    assert(contextDepth > 0)
    assert(archiveDepth >= 1)
    instructionSpec.sanityCheck()
  }
}

// sbt 'testOnly *DualConfigTest'
class DualConfigTest extends AnyFunSuite {

  test("construct config manually") {
    val config = DualConfig(vertexBits = 4, weightBits = 3)
    config.sanityCheck()
  }

  test("construct config incorrectly") {
    assertThrows[AssertionError] {
      // if the weight consists of too many bits to fit into a single message
      val config = new DualConfig(vertexBits = 4, weightBits = 10)
      config.sanityCheck()
    }
  }

  test("construct config from file") {
    val config = new DualConfig(filename = "./resources/graphs/example_code_capacity_d3.json")
    config.sanityCheck()
    assert(config.localIndexOfEdge(vertexIndex = 0, edgeIndex = 0) == 0)
    assert(config.localIndexOfEdge(vertexIndex = 1, edgeIndex = 0) == 0)
    assert(config.localIndexOfEdge(vertexIndex = 1, edgeIndex = 1) == 1)
    assert(config.localIndexOfEdge(vertexIndex = 2, edgeIndex = 1) == 0)
    assert(config.localIndexOfEdge(vertexIndex = 3, edgeIndex = 2) == 0)
    assert(config.localIndexOfEdge(vertexIndex = 0, edgeIndex = 2) == 1)
    assertThrows[Exception] { // exception when the edge is not incident to the vertex
      assert(config.localIndexOfEdge(vertexIndex = 0, edgeIndex = 1) == 0)
    }
  }

}
