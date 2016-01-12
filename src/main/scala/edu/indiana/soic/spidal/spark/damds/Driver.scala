package edu.indiana.soic.spidal.spark.damds

/**
 * Created by pulasthiiu on 10/27/15.
 */

import java.io.IOException
import java.nio.ByteOrder
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import com.google.common.base.{Optional, Stopwatch, Strings}
import edu.indiana.soic.spidal.common.{BinaryReader2D, WeightsWrap, _}
//import edu.indiana.soic.spidal.damds.Utils
//import edu.indiana.soic.spidal.damds.Utils
import edu.indiana.soic.spidal.spark.configurations._
import edu.indiana.soic.spidal.spark.configurations.section._
import org.apache.commons.cli.{Options, _}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.io.Source

object Driver {
  var programOptions: Options = new Options();
  var byteOrder: ByteOrder = null;
  var distances: Array[Array[Short]] = null;
  var weights: WeightsWrap = null;
  var BlockSize: Int = 0;
  var config: DAMDSSection = null;
  var missingDistCount: Accumulator[Int] = null;

  programOptions.addOption(Constants.CmdOptionShortC.toString, Constants.CmdOptionLongC.toString, true, Constants.CmdOptionDescriptionC.toString)
  programOptions.addOption(Constants.CmdOptionShortN.toString, Constants.CmdOptionLongN.toString, true, Constants.CmdOptionDescriptionN.toString)
  programOptions.addOption(Constants.CmdOptionShortT.toString, Constants.CmdOptionLongT.toString, true, Constants.CmdOptionDescriptionT.toString)

  /**
    * Weighted SMACOF based on Deterministic Annealing algorithm
    *
    * @param args command line arguments to the program, which should include
    *             -c path to config file
    *             -t number of threads
    *             -n number of nodes
    *             The options may also be given as longer names
    *             --configFile, --threadCount, and --nodeCount respectively
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkMDS").setMaster("local")
    val sc = new SparkContext(conf)
    val mainTimer: Stopwatch = Stopwatch.createStarted
    var parserResult: Optional[CommandLine] = parseCommandLineArguments(args, Driver.programOptions);


    if (!parserResult.isPresent) {
      println(Constants.ErrProgramArgumentsParsingFailed)
      new HelpFormatter().printHelp(Constants.ProgramName, programOptions)
      return
    }

    var cmd: CommandLine = parserResult.get();
    if (!(cmd.hasOption(Constants.CmdOptionLongC) && cmd.hasOption(Constants.CmdOptionLongN) && cmd.hasOption(Constants.CmdOptionLongT))) {
      System.out.println(Constants.ErrInvalidProgramArguments)
      new HelpFormatter().printHelp(Constants.ProgramName, Driver.programOptions)
      return
    }

    if (!parserResult.isPresent) {
      println(Constants.ErrProgramArgumentsParsingFailed)
      new HelpFormatter().printHelp(Constants.ProgramName, Driver.programOptions)
      return
    }

    //Accumulators
    missingDistCount = sc.accumulator(0, "missingDistCount")

    try {
      readConfigurations(cmd)
      config.distanceMatrixFile = "/home/pulasthi/work/sparkdamds/whiten_dataonly_fullData.2320738e24c8993d6d723d2fd05ca2393bb25fa4.4mer.dist.c#_1000.bin"
      val ranges: Array[Range] = RangePartitioner.Partition(0, 1000, 1)
      ParallelOps.procRowRange = ranges(0);
      readDistancesAndWeights(config.isSammon);

      var rows = matrixToIndexRow(distances)
      var distancesIndexRowMatrix: IndexedRowMatrix = new IndexedRowMatrix(sc.parallelize(rows, 24));
      val distanceSummary: DoubleStatistics = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStatisticsInternal).reduce(combineStatistics);
      val missingDistPercent = missingDistCount.value / (Math.pow(config.numberDataPoints, 2));

      println("\nDistance summary... \n" + distanceSummary.toString + "\n  MissingDistPercentage=" + missingDistPercent)

      weights.setAvgDistForSammon(distanceSummary.getAverage)
      changeZeroDistancesToPostiveMin(distances, distanceSummary.getPositiveMin)

      rows = matrixToIndexRow(distances)
      distancesIndexRowMatrix = new IndexedRowMatrix(sc.parallelize(rows, 24));
      val preX: Array[Array[Double]] = if (Strings.isNullOrEmpty(config.initialPointsFile))
        generateInitMapping(config.numberDataPoints, config.targetDimension)
      else readInitMapping(config.initialPointsFile, config.numberDataPoints, config.targetDimension);
      sc.broadcast(preX);

      var tCur: Double = 0.0
      val tMax: Double = distanceSummary.getMax / Math.sqrt(2.0 * config.targetDimension)
      val tMin: Double = config.tMinFactor * distanceSummary.getPositiveMin / Math.sqrt(2.0 * config.targetDimension)

      var preStress: Double = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStressInternal(preX, config.targetDimension, tCur, null)).
        reduce(_ + _) / distanceSummary.getSumOfSquare;

      println("\nInitial stress=" + preStress)


      mainTimer.stop
      println("\nUp to the loop took " + mainTimer.elapsed(TimeUnit.SECONDS) + " seconds")
      mainTimer.start

      tCur = config.alpha * tMax

      val loopTimer: Stopwatch = Stopwatch.createStarted
      var loopNum: Int = 0
      var diffStress: Double = .0
      var stress: Double = -1.0

      val outRealCGIterations: RefObj[Integer] = new RefObj[Integer](0)
      val cgCount: RefObj[Integer] = new RefObj[Integer](0)

      while (true) {
        preStress = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStressInternal(preX, config.targetDimension, tCur, null)).
          reduce(_ + _) / distanceSummary.getSumOfSquare;

        diffStress = config.threshold + 1.0

        println(String.format("\nStart of loop %d Temperature (T_Cur) %.5g", loopNum, tCur))

        val cgCount: RefObj[Integer] = new RefObj[Integer](0)

        while (diffStress >= config.threshold) {

          // StressLoopTimings.startTiming(StressLoopTimings.TimingTask.BC)
          var BC = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateBCInternal(preX,config.targetDimension,tCur,null,config.blockSize,ParallelOps.globalColCount));
          // StressLoopTimings.endTiming(StressLoopTimings.TimingTask.BC)

          //  StressLoopTimings.startTiming(StressLoopTimings.TimingTask.CG)


          // StressLoopTimings.endTiming(StressLoopTimings.TimingTask.CG)


        }



        print("Asd")
      }
    }catch {
      case e: Exception => {
        e.printStackTrace
      }
    }


    }

    def matrixToIndexRow(matrix: Array[Array[Short]]): Array[IndexedRow] = {
      matrix.zipWithIndex.map { case (row, i) => new IndexedRow(i, new DenseVector(row.map(_.toDouble))) };
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

    def changeZeroDistancesToPostiveMin(distances: Array[Array[Short]], positiveMin: Double)
    {
      var tmpD: Double = 0.0
      for (distanceRow <- distances) {
        for (i <- 0 until distanceRow.length) {
          tmpD = distanceRow(i) * 1.0 / Short.MaxValue
          if (tmpD < positiveMin && tmpD >= 0.0) {
            distanceRow(i) = (positiveMin * Short.MaxValue).toShort
          }
        }
      }
    }

    //TODO need to test method
    def readInitMapping(initialPointsFile: String, numPoints: Int, targetDimension: Int): Array[Array[Double]] =
    {
      try {
        var x: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDimension);
        var line: String = null
        val pattern: Pattern = Pattern.compile("[\t]")
        var row: Int = 0
        for (line <- Source.fromFile(initialPointsFile).getLines()) {
          if (!Strings.isNullOrEmpty(line)) {
            val splits: Array[String] = pattern.split(line.trim)

            for (i <- 0 until splits.length) {
              x(row)(i) = splits(i).trim.toDouble
            }
            row += 1;
          }
        }
        return x;
      } catch {
        case ex: IOException => throw new RuntimeException(ex)
      }
    }

    //TODO need to test method
    def generateInitMapping(numPoints: Int, targetDim: Int): Array[Array[Double]] =
    {
      var x: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDim);
      val rand: Random = new Random(System.currentTimeMillis)
      for (row <- x) {
        for (i <- 0 until row.length) {
          row(i) = if (rand.nextBoolean) rand.nextDouble else -rand.nextDouble
        }
      }
      return x;
    }

    def readConfigurations(cmd: CommandLine): Unit = {
      Driver.config = ConfigurationMgr.LoadConfiguration(
        cmd.getOptionValue(Constants.CmdOptionLongC)).damdsSection;
      //TODO check if this is always correct
      ParallelOps.globalColCount = config.numberDataPoints;
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

    def readDistancesAndWeights(isSammon: Boolean)
    {
      distances = BinaryReader2D.readRowRange(config.distanceMatrixFile, ParallelOps.procRowRange, ParallelOps.globalColCount, byteOrder, true, config.distanceTransform)
      var w: Array[Array[Short]] = null
      if (!Strings.isNullOrEmpty(config.weightMatrixFile)) {
        w = BinaryReader2D.readRowRange(config.weightMatrixFile, ParallelOps.procRowRange, ParallelOps.globalColCount, byteOrder, true, null)
      }
      weights = new WeightsWrap(w, distances, isSammon)
    }

    def calculateStatisticsInternal(index: Int, iter: Iterator[IndexedRow]): Iterator[DoubleStatistics] = {
      var result = List[DoubleStatistics]();
      var missingDistCounts: Int = 0;
      val stats: DoubleStatistics = new DoubleStatistics();
      while (iter.hasNext) {
        val cur = iter.next;
        cur.vector.toArray.map(x => (if ((x * 1.0 / Short.MaxValue) < 0) (missingDistCounts += 1) else (stats.accept((x * 1.0 / Short.MaxValue)))))
      }
      result.::=(stats);
      //TODO test missing distance count
      missingDistCount += missingDistCounts
      result.iterator
    }

    def combineStatistics(doubleStatisticsMain: DoubleStatistics, doubleStatisticsOther: DoubleStatistics): DoubleStatistics = {
      doubleStatisticsMain.combine(doubleStatisticsOther)
      doubleStatisticsMain
    }

    def generateVArrayInternal(index: Int, iter: Iterator[IndexedRow]): Iterator[DoubleStatistics] = {
      null //TODO
    }

    def calculateStressInternal(preX: Array[Array[Double]], targetDimension: Int, tCur: Double,
                                weights: WeightsWrap)(index: Int, iter: Iterator[IndexedRow]): Iterator[Double] = {
      var result = List[Double]()
      var diff: Double = 0.0

      if (tCur > 10E-10) {
        diff = Math.sqrt(2.0 * targetDimension) * tCur
      }
      //TODO support weightsWrap
      val weight: Double = 1.0D
      var localRowCount: Int = 0

      while (iter.hasNext) {
        var sigma: Double = 0.0
        val cur = iter.next;
        val globalRow = index + localRowCount;

        cur.vector.toArray.zipWithIndex.foreach { case (element, index) => {
          var origD = element * 1.0 / Short.MaxValue;
          var euclideanD: Double = if (globalRow != index) calculateEuclideanDist(preX, targetDimension, globalRow, index) else 0.0;
          val heatD: Double = origD - diff
          val tmpD: Double = if (origD >= diff) heatD - euclideanD else -euclideanD
          sigma += weight * tmpD * tmpD
        }
        }

        localRowCount += 1
        result.::=(sigma)
      }
      result.iterator
    }

      def calculateEuclideanDist(vectors: Array[Array[Double]], targetDim: Int, i: Int, j: Int): Double = {
        var dist: Double = 0.0;
        for (k <- 0 until targetDim) {
          val diff: Double = vectors(i)(k) - vectors(j)(k)
          dist += diff * diff
        }
        dist = Math.sqrt(dist)
        dist
      }

  def calculateBCInternal(preX: Array[Array[Double]], targetDimension: Int, tCur: Double,
                          weights: WeightsWrap, blockSize: Int, globalColCount: Int)(index: Int, iter: Iterator[IndexedRow]): Iterator[Array[Double]] ={
    var result = List[Array[Double]]();
    //BCInternalTimings.startTiming(BCInternalTimings.TimingTask.BOFZ, threadIdx)
    var BofZ: Array[Array[Float]] = calculateBofZ(index, iter, preX, targetDimension, tCur, distances, weights, globalColCount)

    //BCInternalTimings.endTiming(BCInternalTimings.TimingTask.BOFZ, threadIdx)

    //BCInternalTimings.startTiming(BCInternalTimings.TimingTask.MM, threadIdx)

    //BCInternalTimings.endTiming(BCInternalTimings.TimingTask.MM, threadIdx)
      result.iterator;
  }

  def calculateBofZ(index: Int,iter: Iterator[IndexedRow], preX: Array[Array[Double]], targetDimension: Int, tCur: Double, distances: Array[Array[Short]], weights: WeightsWrap, globalColCount: Int): Array[Array[Float]] ={
    val vBlockValue: Double = -1
    var diff: Double = 0.0
    val BofZ: Array[Array[Float]] = Array.ofDim[Float](iter.length, globalColCount)
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDimension) * tCur
    }

    var localRow: Int = 0;
    while (iter.hasNext) {
      val cur = iter.next;
      val globalRow: Int = localRow + index;
      cur.vector.toArray.zipWithIndex.foreach { case (element, index) => {
        if (index != globalRow) {
          val origD: Double = element * 1.0 / Short.MaxValue
          val weight: Double = 1.0;


          if (!(origD < 0 || weight == 0)) {
            val dist: Double = calculateEuclideanDist(preX, targetDimension, globalRow, index)

            if (dist >= 1.0E-10 && diff < origD) {
              BofZ(localRow)(index) = (weight * vBlockValue * (origD - diff) / dist).toFloat
            }
            else {
              BofZ(localRow)(index) = 0
            }

            BofZ(localRow)(globalRow) -= BofZ(localRow)(index)
          }

        }
      }


      }
      localRow += 1;
    }

    return BofZ;
  }

  def calculateConjugateGradient(preX: Array[Array[Double]], targetDimension: Int, numPoints: Int, BC: Array[Array[Double]], cgIter: Int, cgThreshold: Double,
                                 outCgCount: RefObj[Integer], outRealCGIterations: RefObj[Integer],
                                 weights: WeightsWrap, blockSize: Int, vArray: Array[Array[Double]]):  Array[Array[Double]] ={
    var X: Array[Array[Double]];


    X;
  }
}