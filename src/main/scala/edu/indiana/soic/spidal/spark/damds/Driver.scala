package edu.indiana.soic.spidal.spark.damds

/**
  * Created by pulasthiiu on 10/27/15.
  */

import java.io._
import java.nio.ByteOrder
import java.text.DecimalFormat
import java.util.{Date, Random}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import com.google.common.base.{Optional, Stopwatch, Strings}
import edu.indiana.soic.spidal.common.{BinaryReader2D, WeightsWrap, _}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.util.control.Breaks._

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
  var procRowOffestsBRMain: Broadcast[Array[Int]] = null;
  var procRowCountsBRMain:  Broadcast[Array[Int]] = null;
  var vArraysBR: Broadcast[Array[Array[Array[Double]]]] = null;
  var vArrayRddsBR: Broadcast[RDD[(Int, Array[Array[Double]])]] = null;

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
    val conf = new SparkConf().setAppName("sparkMDS")
    val sc = new SparkContext(conf)
    val mainTimer: Stopwatch = Stopwatch.createStarted
    val parserResult: Optional[CommandLine] = parseCommandLineArguments(args, Driver.programOptions);

    if (!parserResult.isPresent) {
      println(Constants.ErrProgramArgumentsParsingFailed)
      new HelpFormatter().printHelp(Constants.ProgramName, programOptions)
      return
    }

    val cmd: CommandLine = parserResult.get();
    if (!(cmd.hasOption(Constants.CmdOptionLongC) && cmd.hasOption(Constants.CmdOptionLongN) && cmd.hasOption(Constants.CmdOptionLongT))) {
      println(Constants.ErrInvalidProgramArguments)
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
      val ranges: Array[Range] = RangePartitioner.Partition(0, 1000, 1)
      ParallelOps.procRowRange = ranges(0);
      readDistancesAndWeights(config.isSammon);

      var rows = matrixToIndexRow(distances)
      var distancesIndexRowMatrix: IndexedRowMatrix = new IndexedRowMatrix(sc.parallelize(rows, 24));

      val distanceSummary: DoubleStatistics = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStatisticsInternal(missingDistCount)).reduce(combineStatistics);
      val missingDistPercent = missingDistCount.value / (Math.pow(config.numberDataPoints, 2));
      println("\nDistance summary... \n" + distanceSummary.toString + "\n  MissingDistPercentage=" + missingDistPercent)

      weights.setAvgDistForSammon(distanceSummary.getAverage)
      changeZeroDistancesToPostiveMin(distances, distanceSummary.getPositiveMin)

      rows = matrixToIndexRow(distances)
      distancesIndexRowMatrix = new IndexedRowMatrix(sc.parallelize(rows, 24));
      val countRowTuples = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(countRows).collect()
      calculateRowOffsets(countRowTuples);
      procRowOffestsBRMain = sc.broadcast(procRowOffests);
      procRowCountsBRMain = sc.broadcast(procRowCounts);

      var preX: Array[Array[Double]] = if (Strings.isNullOrEmpty(config.initialPointsFile))
        generateInitMapping(config.numberDataPoints, config.targetDimension)
      else readInitMapping(config.initialPointsFile, config.numberDataPoints, config.targetDimension);
   //  sc.broadcast(preX); //TODO check if Prex can be broadcasted

      var tCur: Double = 0.0
      val tMax: Double = distanceSummary.getMax / Math.sqrt(2.0 * config.targetDimension)
      val tMin: Double = config.tMinFactor * distanceSummary.getPositiveMin / Math.sqrt(2.0 * config.targetDimension)

      vArrayRdds = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(generateVArrayInternal(weights,procRowOffestsBRMain))
      vArrayRddsBR = sc.broadcast(vArrayRdds)

      var preStress: Double = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStressInternal(preX, config.targetDimension, tCur, null,procRowOffestsBRMain)).
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
      var smacofRealIterations: Int = 0
      var X: Array[Array[Double]] = null;
      breakable {
        while (true) {
          preStress = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStressInternal(preX, config.targetDimension, tCur, null, procRowOffestsBRMain)).
            reduce(_ + _) / distanceSummary.getSumOfSquare;

          diffStress = config.threshold + 1.0

          printf("\nStart of loop %d Temperature (T_Cur) %.5g", loopNum, tCur)
          var itrNum: Int = 0
          while (diffStress >= config.threshold) {

            // StressLoopTimings.startTiming(StressLoopTimings.TimingTask.BC)
            var BC = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateBCInternal(preX, config.targetDimension, tCur, null, config.blockSize, ParallelOps.globalColCount, procRowOffestsBRMain)).reduce(mergeBC)
            println("\ncalculated BC ")
            // StressLoopTimings.endTiming(StressLoopTimings.TimingTask.BC)

            //  StressLoopTimings.startTiming(StressLoopTimings.TimingTask.CG)
            //Calculating ConjugateGradient
            X = calculateConjugateGradient(preX, config.targetDimension, config.numberDataPoints,
              BC, config.cgIter, config.cgErrorThreshold, cgCount, outRealCGIterations,
              weights, BlockSize, procRowOffestsBRMain,procRowCountsBRMain);
            // StressLoopTimings.endTiming(StressLoopTimings.TimingTask.CG)
            //StressLoopTimings.startTiming(StressLoopTimings.TimingTask.STRESS)
            stress = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStressInternal(X, config.targetDimension, tCur, null, procRowOffestsBRMain)).
              reduce(_ + _) / distanceSummary.getSumOfSquare;
            //StressLoopTimings.endTiming(StressLoopTimings.TimingTask.STRESS)

            diffStress = preStress - stress
            preStress = stress
            preX = MatrixUtils.copy(X)


            if ((itrNum % 10 == 0) || (itrNum >= config.stressIter)) {
              printf("  Loop %d Iteration %d Avg CG count %.5g " + "Stress " + "%.5g", loopNum, itrNum, (cgCount.getValue * 1.0 / (itrNum + 1)), stress)
            }

            itrNum += 1
            smacofRealIterations += 1


          }
          //TemperatureLoopTimings.endTiming(TemperatureLoopTimings.TimingTask.STRESS_LOOP)
          println("\nEnded BC Loop ")
          var s = 0;

          itrNum -= 1

          if (itrNum >= 0 && !(itrNum % 10 == 0) && !(itrNum >= config.stressIter)) {
            printf("  Loop %d Iteration %d Avg CG count %.5g Stress %.5g", loopNum, itrNum, (cgCount.getValue * 1.0 / (itrNum + 1)), stress)
          }

          printf("End of loop %d Total Iterations %d Avg CG count %.5g Stress %.5g", loopNum, (itrNum + 1), (cgCount.getValue * 1.0 / (itrNum + 1)), stress)
          if (tCur == 0) {
            break
          }
          tCur *= config.alpha
          if (tCur < tMin) {
            tCur = 0
          }
          loopNum += 1
        }
      }
      loopTimer.stop

      val QoR1: Double = stress / (config.numberDataPoints * (config.numberDataPoints - 1) / 2)
      val QoR2: Double = QoR1 / (distanceSummary.getAverage * distanceSummary.getAverage)

      printf("Normalize1 = %.5g Normalize2 = %.5g",QoR1, QoR2)
      printf("Average of Delta(original distance) = %.5g", distanceSummary.getAverage)


      /* TODO Fix error handling here */
      if (Strings.isNullOrEmpty(config.labelFile) || config.labelFile.toUpperCase.endsWith("NOLABEL")) {
        try {
          writeOuput(X, config.pointsFile)
        }
        catch {
          case e: IOException => {
            e.printStackTrace
          }
        }
      } else {
        try {
          writeOuput(X, config.labelFile, config.pointsFile)
        }
        catch {
          case e: IOException => {
            e.printStackTrace
          }
        }
      }

      val finalStress: Double = distancesIndexRowMatrix.rows.mapPartitionsWithIndex(calculateStressInternal(X, config.targetDimension, tCur, null, procRowOffestsBRMain)).
        reduce(_ + _) / distanceSummary.getSumOfSquare;

      mainTimer.stop


      println("Finishing DAMDS run ...")
      val totalTime: Long = mainTimer.elapsed(TimeUnit.MILLISECONDS)
      val temperatureLoopTime: Long = loopTimer.elapsed(TimeUnit.MILLISECONDS)
      printf("  Total Time: %s (%d ms) Loop Time: %s (%d ms)",formatElapsedMillis(totalTime), totalTime, formatElapsedMillis(temperatureLoopTime), temperatureLoopTime)
      printf("  Total Loops: " + loopNum)
      printf("  Total Iterations: " + smacofRealIterations)
      printf("  Total CG Iterations: %d Avg. CG Iterations: %.5g", outRealCGIterations.getValue, (outRealCGIterations.getValue * 1.0) / smacofRealIterations)
      printf("  Final Stress:\t" + finalStress)

      //printTimings(totalTime, temperatureLoopTime)

      println("== DAMDS run completed on " + new Date() + " ==")
    } catch {
      case e: Exception => {
        e.printStackTrace
      }
    }

  }

  def formatElapsedMillis(elapsed: Long): String = {
    val format: String = "%dd:%02dH:%02dM:%02dS:%03dmS"
    val millis: Short = (elapsed % (1000.0)).toShort
    var elapsedlocal: Long = (elapsed - millis) / 1000
    val seconds: Byte = (elapsedlocal % 60.0).toByte
    elapsedlocal = (elapsedlocal - seconds) / 60
    val minutes: Byte = (elapsedlocal % 60.0).toByte
    elapsedlocal = (elapsedlocal - minutes) / 60
    val hours: Byte = (elapsedlocal % 24.0).toByte
    val days: Long = (elapsedlocal - hours) / 24
    return "%dd:%02dH:%02dM:%02dS:%03dmS".format(days, hours, minutes, seconds, millis)
  }

  @throws(classOf[IOException])
  private def writeOuput(x: Array[Array[Double]], labelFile: String, outputFile: String): Unit = {
    val reader: BufferedReader = new BufferedReader(new FileReader(labelFile))
    var line: String = null
    var parts: Array[String] = null
    val labels = new mutable.HashMap[String, Int]()

    while((line = reader.readLine()) != null){
      parts = line.split(" ")
      if (parts.length < 2) {
        System.out.println("ERROR: Invalid label")
      }
      labels.put(parts(0).trim, Integer.valueOf(parts(1)))
    }
    reader.close

    val file: File = new File(outputFile);
    val writer: PrintWriter = new PrintWriter(new FileWriter(file))

    val N: Int = x.length
    val vecLen: Int = x(0).length

    val format: DecimalFormat = new DecimalFormat("#.##########")
    for(i <- 0 until N) {
      writer.print(String.valueOf(i) + '\t') // print ID.
      for (j <- 0 until vecLen) {
        writer.print(format.format(x(i)(j)) + '\t') // print
        // configuration
        // of each axis.
      }
    }
    writer.flush
    writer.close
  }


    @throws(classOf[IOException])
  private def writeOuput(x: Array[Array[Double]], outputFile: String) {
    val writer: PrintWriter = new PrintWriter(new FileWriter(outputFile))
    val N: Int = x.length
    val vecLen: Int = x(0).length
    val format: DecimalFormat = new DecimalFormat("#.##########")

    for(i <- 0 until N){
      writer.print(String.valueOf(i) + '\t')
      for(j <- 0 until vecLen) {
        writer.print(format.format(x(i)(j)) + '\t') // print configuration
        // of each axis.
      }
      writer.println("1") // print label value, which is ONE for all data.
    }
    writer.flush
    writer.close
  }


  def countRows(index: Int, iter: Iterator[IndexedRow]): Iterator[(Int,Int)] = {
    val tuple = new Tuple2(index, iter.length)
    val result = List(tuple);
    result.iterator
  }

//  def mergeRowCounts(values: Array[Int]): Unit = {
//    var index = values(0)
//    var size = values(1);
//    procRowCounts(index) = size;
//  }

  def calculateRowOffsets(tuples: Array[(Int,Int)]): Unit = {
    var rowOffsetTotalCount: Int = 0;

    tuples.foreach(tuple => {
      val index = tuple._1;
      val value = tuple._2;
      procRowCounts(index) = value;
      procRowOffests(index) = rowOffsetTotalCount;
      rowOffsetTotalCount += value;
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

  def calculateStatisticsInternal(missingDistCount: Accumulator[Int])(index: Int, iter: Iterator[IndexedRow]): Iterator[DoubleStatistics] = {

    var result = List[DoubleStatistics]();
    var missingDistCounts: Int = 0;
    val stats: DoubleStatistics = new DoubleStatistics();
    while (iter.hasNext) {
      val cur = iter.next;
      cur.vector.toArray.map(x => (if ((x * 1.0 / Short.MaxValue) < 0) (missingDistCounts += 1) else (stats.accept((x * 1.0 / Short.MaxValue)))))
    }
    result.::=(stats);
    //TODO test missing distance count
    //println("got here")
   /// println("got here" + missingDistCount.value)
    missingDistCount.add(missingDistCounts)
    missingDistCount.add(1)
    //println("got here" + missingDistCount.value)
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

  def generateVArrayInternal(weights: WeightsWrap,procRowOffestsBR: Broadcast[Array[Int]])(index: Int, iter: Iterator[IndexedRow]): Iterator[(Int, Array[Array[Double]])] = {
    val indexRowArray = iter.toArray;
    val vs: Array[Array[Double]] = Array.ofDim[Array[Double]](1);
    val v: Array[Double] = new Array[Double](indexRowArray.length);

    var localRowCount: Int = 0
    indexRowArray.foreach(cur => {
      val globalRow: Int = localRowCount + procRowOffestsBR.value(index);

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

//  def addToVArray(tuples: Array[(Int, Array[Array[Double]])]): Unit = {
//    tuples.foreach(tuple => {
//      vArrays(tuple._1) = tuple._2;
//    })
//  }

  def calculateStressInternal(preX: Array[Array[Double]], targetDimension: Int, tCur: Double,
                              weights: WeightsWrap, procRowOffestsBR: Broadcast[Array[Int]])(index: Int, iter: Iterator[IndexedRow]): Iterator[Double] = {
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
      val globalRow = procRowOffestsBR.value(index) + localRowCount;

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
                          weights: WeightsWrap, blockSize: Int, globalColCount: Int, procRowOffestsBR: Broadcast[Array[Int]])(index: Int, iter: Iterator[IndexedRow]): Iterator[Array[Array[Double]]] = {
    //BCInternalTimings.startTiming(BCInternalTimings.TimingTask.BOFZ, threadIdx)
    var indexRowArray = iter.toArray;
    val BofZ: Array[Array[Float]] = calculateBofZ(index, indexRowArray, preX, targetDimension, tCur, distances, weights, globalColCount, procRowOffestsBR)

    //BCInternalTimings.endTiming(BCInternalTimings.TimingTask.BOFZ, threadIdx)

    //BCInternalTimings.startTiming(BCInternalTimings.TimingTask.MM, threadIdx)
    val multiplyResult: Array[Array[Double]] = Array.ofDim[Double](indexRowArray.length, targetDimension);
    MatrixUtils.matrixMultiply(BofZ, preX, indexRowArray.length, targetDimension, globalColCount, blockSize, multiplyResult);

    //BCInternalTimings.endTiming(BCInternalTimings.TimingTask.MM, threadIdx)
    val result = List(multiplyResult);
    result.iterator;
  }

  def calculateBofZ(index: Int, indexRowArray: Array[IndexedRow], preX: Array[Array[Double]], targetDimension: Int, tCur: Double, distances: Array[Array[Short]], weights: WeightsWrap, globalColCount: Int, procRowOffestsBR: Broadcast[Array[Int]]): Array[Array[Float]] = {
    val vBlockValue: Double = -1
    var diff: Double = 0.0
    val BofZ: Array[Array[Float]] = Array.ofDim[Float](indexRowArray.length, globalColCount)
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDimension) * tCur
    }

    var localRow: Int = 0;
    indexRowArray.foreach(cur => {
      val globalRow: Int = localRow + procRowOffestsBR.value(index);
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
                                 weights: WeightsWrap, blockSize: Int,  procRowOffestsBR: Broadcast[Array[Int]], procRowCountsBR: Broadcast[Array[Int]] ): Array[Array[Double]] = {
    var X: Array[Array[Double]] = preX;
    val p: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDimension)
    var r: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDimension)

    //CGTimings.startTiming(CGTimings.TimingTask.MM)
    var mmtuples = vArrayRddsBR.value.map(calculateMMInternal(X, targetDimension, numPoints, weights, blockSize,procRowOffestsBR, procRowCountsBR)).collect()
    addToMMArray(mmtuples, r,procRowOffestsBR)
    //CGTimings.endTiming(CGTimings.TimingTask.MM)

    for (i <- 0 until numPoints) {
      for (j <- 0 until targetDimension) {
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
    breakable {
      while (cgCount < cgIter) {
        cgCount += 1;
        outRealCGIterations.setValue(outRealCGIterations.getValue + 1)

        //calculate alpha
        //CGLoopTimings.startTiming(CGLoopTimings.TimingTask.MM)
        val Ap: Array[Array[Double]] = Array.ofDim[Double](numPoints, targetDimension)
        var Aptuples = vArrayRddsBR.value.map(calculateMMInternal(p, targetDimension, numPoints, weights, blockSize,procRowOffestsBR, procRowCountsBR)).collect()
        addToMMArray(Aptuples, Ap, procRowOffestsBR)
        //CGLoopTimings.endTiming(CGLoopTimings.TimingTask.MM)

        //CGLoopTimings.startTiming(CGLoopTimings.TimingTask.INNER_PROD_PAP)
        val alpha: Double = rTr / innerProductCalculation(p, Ap)
        //CGLoopTimings.endTiming(CGLoopTimings.TimingTask.INNER_PROD_PAP)


        //update Xi to Xi+1
        for (i <- 0 until numPoints) {
          for (j <- 0 until targetDimension) {
            X(i)(j) += alpha * p(i)(j)
          }
        }

        if (rTr < testEnd) {
          break()
        }

        //update ri to ri+1
        for (i <- 0 until numPoints) {
          for (j <- 0 until targetDimension) {
            r(i)(j) = r(i)(j) - alpha * Ap(i)(j)
          }
        }

        //calculate beta
        //CGLoopTimings.startTiming(CGLoopTimings.TimingTask.INNER_PROD_R)
        val rTr1: Double = innerProductCalculation(r)
        //CGLoopTimings.endTiming(CGLoopTimings.TimingTask.INNER_PROD_R)
        val beta: Double = rTr1 / rTr
        rTr = rTr1

        //update pi to pi+1
        for (i <- 0 until numPoints) {
          for (j <- 0 until targetDimension) {
            p(i)(j) = r(i)(j) + beta * p(i)(j)
          }
        }
      }
    }

    //CGTimings.endTiming(CGTimings.TimingTask.CG_LOOP)
    outCgCount.setValue(outCgCount.getValue + cgCount)
    X;
  }

  def calculateMMInternal(x: Array[Array[Double]], targetDimension: Int, numPoints: Int, weights: WeightsWrap,
                          blockSize: Int, procRowOffestsBR: Broadcast[Array[Int]], procRowCountsBR: Broadcast[Array[Int]])(tuple: (Int, Array[Array[Double]])): (Int, Array[Array[Double]]) = {
    var index = tuple._1;
    var vArray = tuple._2;
    var mm: Array[Array[Double]] = Array.ofDim[Double](procRowCountsBR.value(index), targetDimension)
    MatrixUtils.matrixMultiplyWithThreadOffset(weights, vArray(0), x, procRowCountsBR.value(index), targetDimension, numPoints, blockSize, 0, procRowOffestsBR.value(index), mm);
    (index, mm)
  }

  def addToMMArray(tuples: Array[(Int, Array[Array[Double]])], out: Array[Array[Double]], procRowOffestsBR: Broadcast[Array[Int]]): Unit = {
    tuples.foreach(tuple => {
      var index = tuple._1;
      var rowcount = 0;
      tuple._2.foreach(row => {
        out(procRowOffestsBR.value(index) + rowcount) = tuple._2(rowcount);
        rowcount += 1;
      })
    })
  }

  def innerProductCalculation(a: Array[Array[Double]], b: Array[Array[Double]]): Double = {
    var sum: Double = 0.0;

    if (a.length > 0) {
      var col: Int = a(0).length
      var row: Int = a.length

      for (i <- 0 until row) {
        for (j <- 0 until col) {
          sum += a(i)(j) * b(i)(j)
        }
      }
    }
    sum
  }

  def innerProductCalculation(a: Array[Array[Double]]): Double = {
    var sum: Double = 0.0;

    if (a.length > 0) {
      var col: Int = a(0).length;
      a.foreach(anA => {
        for (i <- 0 until col) {
          sum += anA(i) * anA(i)
        }
      });
    }
    sum
  }
}