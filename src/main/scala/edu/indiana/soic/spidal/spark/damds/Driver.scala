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
import org.apache.spark.rdd.RDD

//import edu.indiana.soic.spidal.damds.Utils
//import edu.indiana.soic.spidal.damds.Utils
import edu.indiana.soic.spidal.spark.configurations._
import edu.indiana.soic.spidal.spark.configurations.section._
import org.apache.commons.cli.{Options, _}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.{Partition, Accumulator, SparkConf, SparkContext}

import scala.io.Source

object Driver {
  var programOptions: Options = new Options();
  var byteOrder: ByteOrder = null;
  var distances: Array[Array[Short]] = null;
  var weights: WeightsWrap = null;
  var BlockSize: Int = 0;
  var config: DAMDSSection = null;
  var missingDistCount: Accumulator[Int] = null;
  var procRowCounts: Array[Int] = new Array[Int](24)
  var procRowOffests: Array[Int] = new Array[Int](24);
  var vArrays: Array[Array[Array[Double]]] = new Array[Array[Array[Double]]](24);
  var vArrayRdds: RDD[(Int, Array[Array[Double]])] = null;

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
      distancesIndexRowMatrix.rows.mapPartitionsWithIndex(countRows).foreach(mergeRowCounts);
      calculateRowOffsets();
      sc.broadcast(procRowOffests)
      val preX: Array[Array[Double]] = if (Strings.isNullOrEmpty(config.initialPointsFile))
        generateInitMapping(config.numberDataPoints, config.targetDimension)
      else readInitMapping(config.initialPointsFile, config.numberDataPoints, config.targetDimension);
      sc.broadcast(preX);

      var tCur: Double = 0.0
      val tMax: Double = distanceSummary.getMax / Math.sqrt(2.0 * config.targetDimension)
      val tMin: Double = config.tMinFactor * distanceSummary.getPositiveMin / Math.sqrt(2.0 * config.targetDimension)

      var vArrayTuples = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(generateVArrayInternal(weights)).collect();
     // vArrayRdds = sc.parallelize(vArrayTuples,24);
      vArrayRdds = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(generateVArrayInternal(weights))
     // var partitions = vArrayRdds.partitions;
      addToVArray(vArrayTuples);
      sc.broadcast(vArrays)
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

      //while (true) {
      preStress = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStressInternal(preX, config.targetDimension, tCur, null)).
        reduce(_ + _) / distanceSummary.getSumOfSquare;

      diffStress = config.threshold + 1.0

      //      println(String.format("\nStart of loop %d Temperature (T_Cur) %.5g", loopNum, tCur))

      // while (diffStress >= config.threshold) {

      // StressLoopTimings.startTiming(StressLoopTimings.TimingTask.BC)
      var BC = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateBCInternal(preX, config.targetDimension, tCur, null, config.blockSize, ParallelOps.globalColCount)).reduce(mergeBC)
      println("\ncalculated BC ")
     // println(BC.deep.toString)
      // StressLoopTimings.endTiming(StressLoopTimings.TimingTask.BC)

      //  StressLoopTimings.startTiming(StressLoopTimings.TimingTask.CG)
      //Calculating ConjugateGradient
      calculateConjugateGradient(preX, config.targetDimension, config.numberDataPoints,
        BC, config.cgIter, config.cgErrorThreshold, cgCount, outRealCGIterations,
        weights, BlockSize);
      // StressLoopTimings.endTiming(StressLoopTimings.TimingTask.CG)


      //    }
      //}
    } catch {
      case e: Exception => {
        e.printStackTrace
      }
    }


  }

//  def populateRowOffsets(matrix : IndexedRowMatrix): Unit ={
//    matrix.rows.
//    var partitions: Array[Partition] = matrix.rows.partitions;
//    partitions.foreach( partition => {
//      var index = partition.index;
//      var numberofRows = partition.
//    })
//  }

  def countRows(index: Int, iter: Iterator[IndexedRow]): Iterator[Array[Int]] ={
    var result = List[Array[Int]]();
    var values = new Array[Int](2);
    values(0) = index;
    values(1) = iter.length;
    result.::=(values)
    result.iterator
  }

  def mergeRowCounts(values: Array[Int]): Unit ={
    var index = values(0)
    var size = values(1);
    procRowCounts(index) = size;
  }

  def calculateRowOffsets(): Unit ={
    var rowOffsetTotalCount: Int = 0;
    var count = 0;
    procRowCounts.foreach( element => {
      procRowOffests(count) = rowOffsetTotalCount;
      rowOffsetTotalCount += element;
      count += 1;
    })
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

  //    def matrixToBlockMatrix(matrix : Array[Array[Float]]): BlockMatrix {
  //
  //    }

  def changeZeroDistancesToPostiveMin(distances: Array[Array[Short]], positiveMin: Double) {
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
  def readInitMapping(initialPointsFile: String, numPoints: Int, targetDimension: Int): Array[Array[Double]] = {
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
  def generateInitMapping(numPoints: Int, targetDim: Int): Array[Array[Double]] = {
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

  def readDistancesAndWeights(isSammon: Boolean) {
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

  //TODO check if using a foreach and seperatly copying rows is faster
  def mergeBC(bcmain: Array[Array[Double]], bcother: Array[Array[Double]]): Array[Array[Double]] = {
    var bc = Array.concat(bcmain, bcother);
    bc;
  }

  def generateVArrayInternal(weights: WeightsWrap)(index: Int, iter: Iterator[IndexedRow]): Iterator[(Int, Array[Array[Double]])] = {
    val indexRowArray = iter.toArray;
    val vs: Array[Array[Double]] = Array.ofDim[Array[Double]](1);
    val v: Array[Double] = new Array[Double](indexRowArray.length);

    var localRowCount: Int = 0
    indexRowArray.foreach(cur => {
      val globalRow: Int = localRowCount + procRowOffests(index);

      cur.vector.toArray.zipWithIndex.foreach { case (element, globalColumn) => {
        if (globalRow != globalColumn) {
          val origD = element * 1.0 / Short.MaxValue
          val weight: Double = 1.0;

          if (!(origD < 0 || weight == 0)) {
            v(localRowCount) += weight;
          }

        }
      }
        v(localRowCount) += 1;
      }
      localRowCount += 1;
    });
    vs(0) = v;
    val tuple = new Tuple2(index, vs)
    val result = List(tuple);
    result.iterator
  }

  def addToVArray(tuples: Array[(Int, Array[Array[Double]])]): Unit ={
    tuples.foreach( tuple => {
      vArrays(tuple._1) = tuple._2;
    })
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
      val globalRow = procRowOffests(index) + localRowCount;

      cur.vector.toArray.zipWithIndex.foreach { case (element, globalColumn) => {
        var origD = element * 1.0 / Short.MaxValue;
        var euclideanD: Double = if (globalRow != globalColumn) calculateEuclideanDist(preX, targetDimension, globalRow, globalColumn) else 0.0;
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
                          weights: WeightsWrap, blockSize: Int, globalColCount: Int)(index: Int, iter: Iterator[IndexedRow]): Iterator[Array[Array[Double]]] = {
    //BCInternalTimings.startTiming(BCInternalTimings.TimingTask.BOFZ, threadIdx)
    var indexRowArray = iter.toArray;
    val BofZ: Array[Array[Float]] = calculateBofZ(index, indexRowArray, preX, targetDimension, tCur, distances, weights, globalColCount)

    //BCInternalTimings.endTiming(BCInternalTimings.TimingTask.BOFZ, threadIdx)

    //BCInternalTimings.startTiming(BCInternalTimings.TimingTask.MM, threadIdx)
    val multiplyResult: Array[Array[Double]] = Array.ofDim[Double](indexRowArray.length, targetDimension);
    MatrixUtils.matrixMultiply(BofZ, preX, indexRowArray.length, targetDimension, globalColCount, blockSize, multiplyResult);

    //BCInternalTimings.endTiming(BCInternalTimings.TimingTask.MM, threadIdx)
    val result = List(multiplyResult);
    result.iterator;
  }

  def calculateBofZ(index: Int, indexRowArray: Array[IndexedRow], preX: Array[Array[Double]], targetDimension: Int, tCur: Double, distances: Array[Array[Short]], weights: WeightsWrap, globalColCount: Int): Array[Array[Float]] = {
    val vBlockValue: Double = -1
    var diff: Double = 0.0
    val BofZ: Array[Array[Float]] = Array.ofDim[Float](indexRowArray.length, globalColCount)
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDimension) * tCur
    }

    var localRow: Int = 0;
    indexRowArray.foreach(cur => {
      val globalRow: Int = localRow + procRowOffests(index);
      cur.vector.toArray.zipWithIndex.foreach { case (element, column) => {
        if (column != globalRow) {
          val origD: Double = element * 1.0 / Short.MaxValue
          val weight: Double = 1.0;


          if (!(origD < 0 || weight == 0)) {
            val dist: Double = calculateEuclideanDist(preX, targetDimension, globalRow, column)

            if (dist >= 1.0E-10 && diff < origD) {
              BofZ(localRow)(column) = (weight * vBlockValue * (origD - diff) / dist).toFloat
            }
            else {
              BofZ(localRow)(column) = 0
            }

            BofZ(localRow)(globalRow) -= BofZ(localRow)(column)
          }

        }
      }


      }
      localRow += 1;
    })

    return BofZ;
  }

  def calculateConjugateGradient(preX: Array[Array[Double]], targetDimension: Int, numPoints: Int, BC: Array[Array[Double]], cgIter: Int, cgThreshold: Double,
                                 outCgCount: RefObj[Integer], outRealCGIterations: RefObj[Integer],
                                 weights: WeightsWrap, blockSize: Int): Array[Array[Double]] = {
    var X: Array[Array[Double]] = preX;
    val p: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDimension)
    var r: Array[Array[Double]] = Array.ofDim[Double](numPoints,targetDimension)

    //CGTimings.startTiming(CGTimings.TimingTask.MM)
    var mmtuples = vArrayRdds.map(calculateMMInternal(X,targetDimension,numPoints,weights,blockSize)).collect()
    addToMMArray(mmtuples,r)
    //CGTimings.endTiming(CGTimings.TimingTask.MM)

    for( i <- 0 until numPoints){
      for( j <- 0 until targetDimension) {
        p(i)(j) = BC(i)(j) - r(i)(j)
        r(i)(j) = p(i)(j)
      }
    }

    var cgCount: Int = 0
    //CGTimings.startTiming(CGTimings.TimingTask.INNER_PROD)
    var rTr: Double = innerProductCalculation(r)
    //CGTimings.endTiming(CGTimings.TimingTask.INNER_PROD)

    // Adding relative value test for termination as suggested by Dr. Fox.
    val testEnd: Double = rTr * cgThreshold

    //CGTimings.startTiming(CGTimings.TimingTask.CG_LOOP)
    while (cgCount < cgIter) {
      cgCount += 1;
      outRealCGIterations.setValue(outRealCGIterations.getValue + 1)

      //calculate alpha
      //CGLoopTimings.startTiming(CGLoopTimings.TimingTask.MM)
      val Ap: Array[Array[Double]] = Array.ofDim[Double](numPoints,targetDimension)
      var Aptuples = vArrayRdds.map(calculateMMInternal(p,targetDimension,numPoints,weights,blockSize)).collect()
      addToMMArray(Aptuples,Ap)
      //CGLoopTimings.endTiming(CGLoopTimings.TimingTask.MM)

      //CGLoopTimings.startTiming(CGLoopTimings.TimingTask.INNER_PROD_PAP)
      val alpha: Double = rTr / innerProductCalculation(p, Ap)
      //CGLoopTimings.endTiming(CGLoopTimings.TimingTask.INNER_PROD_PAP)


    }
    X;
  }

  def calculateMMInternal(x: Array[Array[Double]], targetDimension: Int, numPoints: Int, weights: WeightsWrap,
                          blockSize: Int)(tuple: (Int, Array[Array[Double]])): (Int, Array[Array[Double]]) ={
    var index = tuple._1;
    var vArray = tuple._2;
    var mm: Array[Array[Double]] = Array.ofDim[Double]( procRowCounts(index),targetDimension)
    MatrixUtils.matrixMultiplyWithThreadOffset(weights, vArray(0), x, procRowCounts(index), targetDimension, numPoints, blockSize, 0, procRowOffests(index),mm);
    (index, mm)
  }

  def addToMMArray(tuples: Array[(Int, Array[Array[Double]])], out: Array[Array[Double]]): Unit ={
    tuples.foreach( tuple => {
      var index =  tuple._1;
      var rowcount = 0;
      tuple._2.foreach(row => {
        out(procRowOffests(index)+rowcount) = tuple._2(rowcount);
        rowcount += 1;
      })
    })
  }

  def innerProductCalculation(a: Array[Array[Double]],b: Array[Array[Double]]): Double ={
    var sum: Double = 0.0;

    if(a.length > 0){
      var col: Int = a(0).length
      var row: Int = a.length

      for( i <- 0 until row){
        for( j <- 0 until col) {
          sum += a(i)(j) * b(i)(j)
        }
      }
    }
    sum
  }

  def innerProductCalculation(a : Array[Array[Double]]): Double ={
    var sum: Double = 0.0;

    if(a.length > 0){
      var col: Int = a(0).length;
      a.foreach(anA => {
        for(i <- 0 until col){
          sum += anA(i) * anA(i)
        }
      });
    }
    sum
  }
  //  def calculateMMInternal(x: Array[Array[Double]], targetDimension: Int, numPoints: Int,
  //                          weights: WeightsWrap, blockSize: Int)(index: Int, iter:Iterator[IndexedRow]): Iterator[Array[Array[Double]]] ={
  //
  //
  //  }
}