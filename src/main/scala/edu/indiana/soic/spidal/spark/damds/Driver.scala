package edu.indiana.soic.spidal.spark.damds

/**
 * Created by pulasthiiu on 10/27/15.
 */

import java.nio.ByteOrder
import org.apache.commons.cli._
import edu.indiana.soic.spidal.common._
import com.google.common.base.{Strings, Optional}
import edu.indiana.soic.spidal.common.{BinaryReader2D, WeightsWrap}
import org.apache.commons.cli.Options
import edu.indiana.soic.spidal.spark.configurations.section._;
import edu.indiana.soic.spidal.spark.configurations._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.{Accumulator, SparkContext, SparkConf}
;

object Driver {
  var programOptions: Options = new Options();
  var byteOrder: ByteOrder = null;
  var distances: Array[Array[Short]] = null;
  var weights: WeightsWrap = null;
  var BlockSize: Int = 0;
  var config: DAMDSSection = null;
  var missingDistCount : Accumulator[Int] = null;
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkMDS").setMaster("local")
    val sc = new SparkContext(conf)
    var parserResult: Optional[CommandLine] = parseCommandLineArguments(args, Driver.programOptions);
    //Accumilators
    missingDistCount = sc.accumulator(0, "missingDistCount")


    if (!parserResult.isPresent) {
      println(Constants.ErrProgramArgumentsParsingFailed)
      new HelpFormatter().printHelp(Constants.ProgramName, Driver.programOptions)
      return
    }

//    var cmd: CommandLine = parserResult.get();
//    if (!(cmd.hasOption(Constants.CmdOptionLongC) && cmd.hasOption(Constants.CmdOptionLongN) && cmd.hasOption(Constants.CmdOptionLongT))) {
//      System.out.println(Constants.ErrInvalidProgramArguments)
//      new HelpFormatter().printHelp(Constants.ProgramName, Driver.programOptions)
//      return
//    }

    try {
      config = new DAMDSSection("/home/pulasthi/iuwork/labwork/config.properties");
      println(config.numberDataPoints+"---"+config.isBigEndian)
      ParallelOps.globalColCount = 1000
      config.distanceMatrixFile = "/home/pulasthi/iuwork/labwork/whiten_dataonly_fullData.2320738e24c8993d6d723d2fd05ca2393bb25fa4.4mer.dist.c#_1000.bin"
      val ranges: Array[Range] = RangePartitioner.Partition(0,1000,1)
      ParallelOps.procRowRange = ranges(0);
      readDistancesAndWeights(false);

      val rows = matrixToIndexRow(distances)
      val indexrowmetrix: IndexedRowMatrix = new IndexedRowMatrix(sc.parallelize(rows,24));
      //val test  =  new IndexedRowMatrix(sc.parallelize(indexrowmetrix.rows.mapPartitionsWithIndex(myfunc).collect()));
      val test  =  indexrowmetrix.rows.mapPartitionsWithIndex(calculateStatisticsInternal).reduce(combineStatistics);
      println("....................................")

    } catch {
      case e: Exception => {
        e.printStackTrace
      }
    }


  }

  def matrixToIndexRow (matrix:  Array[Array[Short]]): Array[IndexedRow] = {
    matrix.zipWithIndex.map{case (row,i) => new IndexedRow(i, new DenseVector(row.map(_.toDouble)))};
  }

  def parseCommandLineArguments(args: Array[String], opts: Options): Optional[CommandLine] = {
    val optParser: CommandLineParser = new GnuParser();
    try {
      return Optional.fromNullable(optParser.parse(opts, args))
    }
    catch {
      case e: ParseException => {
        e.printStackTrace
      }
    }
    return Optional.fromNullable(null);
  }
  def readConfigurations(cmd: CommandLine): Unit = {
    Driver.config = ConfigurationMgr.LoadConfiguration(
      cmd.getOptionValue(Constants.CmdOptionLongC)).damdsSection;

    ParallelOps.nodeCount =
      Integer.parseInt(cmd.getOptionValue(Constants.CmdOptionLongN));
    ParallelOps.threadCount =
      Integer.parseInt(cmd.getOptionValue(Constants.CmdOptionLongT));
    ParallelOps.mmapsPerNode = if (cmd.hasOption(Constants.CmdOptionShortMMaps)) cmd.getOptionValue(Constants.CmdOptionShortMMaps).toInt else 1;
    ParallelOps.mmapScratchDir = if (cmd.hasOption(Constants.CmdOptionShortMMapScrathDir)) cmd.getOptionValue(Constants.CmdOptionShortMMapScrathDir) else ".";

    Driver.byteOrder =
      if (Driver.config.isBigEndian) ByteOrder.BIG_ENDIAN else ByteOrder.LITTLE_ENDIAN;
    Driver.BlockSize = Driver.config.blockSize;
  }

  private def readDistancesAndWeights(isSammon: Boolean) {
    distances = BinaryReader2D.readRowRange(config.distanceMatrixFile, ParallelOps.procRowRange, ParallelOps.globalColCount, byteOrder, true, config.distanceTransform)
    var w: Array[Array[Short]] = null
    if (!Strings.isNullOrEmpty(config.weightMatrixFile)) {
      w = BinaryReader2D.readRowRange(config.weightMatrixFile, ParallelOps.procRowRange, ParallelOps.globalColCount, byteOrder, true, null)
    }
    weights = new WeightsWrap(w, distances, isSammon)
  }

  def calculateStatisticsInternal(index: Int, iter: Iterator[IndexedRow]): Iterator[DoubleStatistics] = {
    var res = List[DoubleStatistics]();
    var missingDistCounts: Int = 0;
    val stats: DoubleStatistics = new DoubleStatistics();
    while (iter.hasNext){
      val cur = iter.next;
      cur.vector.toArray.map(x => (if (x < 0) (missingDistCounts += 1) else (stats.accept(x))))
    }
    res .::= (stats);
    //TODO test missing distance count
    missingDistCount += missingDistCounts
    res.iterator
  }

  def combineStatistics(doubleStatisticsMain: DoubleStatistics, doubleStatisticsOther: DoubleStatistics) : DoubleStatistics ={
    doubleStatisticsMain.combine(doubleStatisticsOther)
    doubleStatisticsMain
  }
}

