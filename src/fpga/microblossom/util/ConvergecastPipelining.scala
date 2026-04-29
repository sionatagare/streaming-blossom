package microblossom.util

/**
  * Pure helpers for inserting `RegNext` registers into the convergecast reduction trees.
  *
  * The trees produced by `inferred_from_positions` are NOT balanced — odd-out leaves get
  * carried up unchanged, so leaf-to-root paths can have very different lengths. We pick a
  * set of depths at which to register, then compute the actual cumulative output delay at
  * every node so the caller can pad the shallower side at each merge with extra `Delay`s.
  *
  * Semantics:
  *   - `depths(i)` = max distance from node `i` to any leaf below (leaves = 0).
  *   - `pipelineDepthSet(maxDepth, stages)` = `stages` depths evenly spaced in (0, maxDepth)
  *     where a `RegNext` is inserted at the merge.
  *   - `outputDelays(i)` = total cumulative cycles that data takes from any leaf to the
  *     output of node `i` (after its own optional RegNext, and after aligning its children).
  *     For an internal node: `outputDelays(i) = max(outputDelays(left), outputDelays(right))
  *                                                + (if depth(i) is pipelined then 1 else 0)`.
  *   - `rootDelay` = `outputDelays(last)`.
  */
object ConvergecastPipelining {

  /** Bottom-up depth: leaves = 0, internal nodes = 1 + max(child depths).
    * Assumes tree storage where every internal node's children have smaller indices. */
  def computeTreeDepths(nodes: Seq[BinaryTreeNode]): Array[Int] = {
    val depths = new Array[Int](nodes.length)
    for (i <- nodes.indices) {
      val n = nodes(i)
      (n.l, n.r) match {
        case (Some(lv), Some(rv)) =>
          depths(i) = 1 + scala.math.max(depths(lv.toInt), depths(rv.toInt))
        case _ => depths(i) = 0
      }
    }
    depths
  }

  /** `stages` depths in [1, maxDepth-1], evenly spaced.
    *   stages=1, maxDepth=10 → {5}
    *   stages=2, maxDepth=12 → {4, 8}
    *   stages=0  or  maxDepth<=1  → empty (no pipelining). */
  def pipelineDepthSet(maxDepth: Int, stages: Int): Set[Int] = {
    if (stages <= 0 || maxDepth <= 1) Set.empty
    else (1 to stages).map(i => (i * maxDepth / (stages + 1)).max(1)).toSet
  }

  /** Cumulative leaf-to-output delay per node, accounting for per-merge alignment + RegNext.
    * Assumes children indices < parent index (bottom-up storage). */
  def computeOutputDelays(
      nodes: Seq[BinaryTreeNode],
      depths: Array[Int],
      pipelineDepths: Set[Int]
  ): Array[Int] = {
    val out = new Array[Int](nodes.length)
    for (i <- nodes.indices) {
      val n = nodes(i)
      (n.l, n.r) match {
        case (Some(lv), Some(rv)) =>
          val align = scala.math.max(out(lv.toInt), out(rv.toInt))
          out(i) = align + (if (pipelineDepths.contains(depths(i))) 1 else 0)
        case _ =>
          out(i) = 0
      }
    }
    out
  }

  /** Convenience: end-to-end pipeline delay at the root of the given tree. */
  def rootDelay(nodes: Seq[BinaryTreeNode], stages: Int): Int = {
    if (nodes.isEmpty) return 0
    val depths = computeTreeDepths(nodes)
    val maxDepth = depths.max
    val pipelineDepths = pipelineDepthSet(maxDepth, stages)
    val out = computeOutputDelays(nodes, depths, pipelineDepths)
    out(out.length - 1)
  }
}
