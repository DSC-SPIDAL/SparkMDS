package edu.indiana.soic.spidal.spark.damds

import edu.indiana.soic.spidal.common.Range

/**
 * Created by pulasthiiu on 10/27/15.
 */
object ParallelOps{
  var nodeCount: Int = 1
  var threadCount: Int = 1
  var mmapsPerNode: Int = 0
  var mmapScratchDir: String = ""
  var procRowRange: Range = null
  var globalColCount: Int = 0
}
class ParallelOps {

}