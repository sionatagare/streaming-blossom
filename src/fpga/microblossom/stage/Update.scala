package microblossom.stage

import microblossom._
import microblossom.types._
import microblossom.combinatorial._
import spinal.core._
import spinal.lib._

/*
 * Vertex
 */

case class StageUpdateVertex(config: DualConfig, vertexIndex: Int) extends Bundle {
  private val elastic = config.vertexHasElasticLayers(vertexIndex)
  val state = VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
  val archivedState = elastic generate VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
  val isStalled = Bool
  val compact = BroadcastCompact(config)
  val propagatingPeer = VertexPropagatingPeerResult(config.vertexBits)
  val archivedPropagatingPeer = elastic generate VertexPropagatingPeerResult(config.vertexBits)

  def connect(last: StageExecuteVertex3) = {
    state := last.state
    if (elastic) archivedState := last.archivedState
    isStalled := last.isStalled
    compact := last.compact
  }
}

case class StageUpdateVertex2(config: DualConfig, vertexIndex: Int) extends Bundle {
  private val elastic = config.vertexHasElasticLayers(vertexIndex)
  val state = VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
  val archivedState = elastic generate VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
  val isStalled = Bool
  val compact = BroadcastCompact(config)
  val propagatingPeer = VertexPropagatingPeerResult(config.vertexBits)
  val archivedPropagatingPeer = elastic generate VertexPropagatingPeerResult(config.vertexBits)

  def connect(last: StageUpdateVertex) = {
    compact := last.compact
    isStalled := last.isStalled
    propagatingPeer := last.propagatingPeer
    if (elastic) archivedPropagatingPeer := last.archivedPropagatingPeer
  }
}

case class StageUpdateVertex3(config: DualConfig, vertexIndex: Int) extends Bundle {
  private val elastic = config.vertexHasElasticLayers(vertexIndex)
  val state = VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
  val archivedState = elastic generate VertexState(config.vertexBits, config.grownBitsOf(vertexIndex))
  val shadow = VertexShadowResult(config.vertexBits)
  val archivedShadow = elastic generate VertexShadowResult(config.vertexBits)
  val compact = BroadcastCompact(config)

  def connect(last: StageUpdateVertex2) = {
    state := last.state
    if (elastic) archivedState := last.archivedState
    compact := last.compact
  }
}

/*
 * Offloader
 */

case class StageUpdateOffloader(config: DualConfig, offloaderIndex: Int) extends Bundle {
  val condition = Bool
  def connect(last: StageExecuteOffloader3) = {
    condition := last.condition
  }
}
case class StageUpdateOffloader2(config: DualConfig, offloaderIndex: Int) extends Bundle {
  val condition = Bool
  def connect(last: StageUpdateOffloader) = {
    condition := last.condition
  }
}
case class StageUpdateOffloader3(config: DualConfig, offloaderIndex: Int) extends Bundle {
  val condition = Bool
  def connect(last: StageUpdateOffloader2) = {
    condition := last.condition
  }
}

/*
 * Edge
 */

case class StageUpdateEdge(config: DualConfig) extends Bundle {
  val state = EdgeState(config.weightBits)
  val remaining = UInt(config.weightBits bits)
  val remainingVsElasticLayers = UInt(config.weightBits bits)
  val compact = BroadcastCompact(config)

  def connect(last: StageExecuteEdge3) = {
    state := last.state
    compact := last.compact
  }
}

case class StageUpdateEdge2(config: DualConfig) extends Bundle {
  val state = EdgeState(config.weightBits)
  val remaining = UInt(config.weightBits bits)
  val remainingVsElasticLayers = UInt(config.weightBits bits)
  val compact = BroadcastCompact(config)

  def connect(last: StageUpdateEdge) = {
    state := last.state
    remaining := last.remaining
    remainingVsElasticLayers := last.remainingVsElasticLayers
    compact := last.compact
  }
}

case class StageUpdateEdge3(config: DualConfig) extends Bundle {
  val state = EdgeState(config.weightBits)
  val remaining = UInt(config.weightBits bits)
  val remainingVsElasticLayers = UInt(config.weightBits bits)
  val compact = BroadcastCompact(config)

  def connect(last: StageUpdateEdge2) = {
    state := last.state
    remaining := last.remaining
    remainingVsElasticLayers := last.remainingVsElasticLayers
    compact := last.compact
  }
}
