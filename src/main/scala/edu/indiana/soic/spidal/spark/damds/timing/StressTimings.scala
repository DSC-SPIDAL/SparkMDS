package edu.indiana.soic.spidal.spark.damds.timing

import com.google.common.base.Stopwatch
import java.util.concurrent.TimeUnit
import java.util.Arrays

/**
  * Created by pulasthi on 5/28/16.
  */
object StressTimings {

  object TimingTask extends Enumeration {
    type TimingTask = Value
    val STRESS_INTERNAL, COMM, STRESS_MERGE, STRESS_EXTRACT = Value
  }

  private var numThreads: Int = 0

  def init(numThreads: Int) {
    timerStressInternal = new Array[Stopwatch](numThreads)
    for(i <- 0 until numThreads){
      timerStressInternal(i) = new Stopwatch();
    }
    tStressInternal = new Array[Long](numThreads)
    countStressInternal = new Array[Long](numThreads)
    StressTimings.numThreads = numThreads
  }

  private var timerStressInternal: Array[Stopwatch] = null
  private var timerComm: Stopwatch = new Stopwatch();
  private var timerBCMerge: Stopwatch = new Stopwatch();
  private var timerBCExtract: Stopwatch = new Stopwatch();
  private var tStressInternal: Array[Long] = null
  private var tComm: Long = 0L
  private var tBCMerge: Long = 0L
  private var tBCExtract: Long = 0L
  private var countStressInternal: Array[Long] = null
  private var countComm: Long = 0L
  private var countBCMerge: Long = 0L
  private var countBCExtract: Long = 0L

  def startTiming(task: TimingTask.TimingTask, threadIdx: Int) {
    task match {
      case TimingTask.STRESS_INTERNAL =>
        timerStressInternal(threadIdx).start
        countStressInternal(threadIdx) += 1
      case TimingTask.COMM =>
        timerComm.start
        countComm += 1
      case TimingTask.STRESS_MERGE =>
        timerBCMerge.start
        countBCMerge += 1
      case TimingTask.STRESS_EXTRACT =>
        timerBCExtract.start
        countBCExtract += 1
    }
  }

  def endTiming(task: TimingTask.TimingTask, threadIdx: Int) {
    task match {
      case TimingTask.STRESS_INTERNAL =>
        timerStressInternal(threadIdx).stop
        tStressInternal(threadIdx) += timerStressInternal(threadIdx).elapsed(TimeUnit.MILLISECONDS)
        timerStressInternal(threadIdx).reset
      case TimingTask.COMM =>
        timerComm.stop
        tComm += timerComm.elapsed(TimeUnit.MILLISECONDS)
        timerComm.reset
      case TimingTask.STRESS_MERGE =>
        timerBCMerge.stop
        tBCMerge += timerBCMerge.elapsed(TimeUnit.MILLISECONDS)
        timerBCMerge.reset
      case TimingTask.STRESS_EXTRACT =>
        timerBCExtract.stop
        tBCExtract += timerBCExtract.elapsed(TimeUnit.MILLISECONDS)
        timerBCExtract.reset
    }
  }

  def getTotalTime(task: TimingTask.TimingTask): Double = {
    task match {
      case TimingTask.STRESS_INTERNAL =>
//        return Arrays.stream(tStressInternal).reduce(0, (i, j) => i + j)
        return 0.0
      case TimingTask.COMM =>
        return tComm
      case TimingTask.STRESS_MERGE =>
        return tBCMerge
      case TimingTask.STRESS_EXTRACT =>
        return tBCExtract
    }
    return 0.0
  }

  def getAverageTime(task: TimingTask.TimingTask): Double = {
    task match {
      case TimingTask.STRESS_INTERNAL =>
//        return Arrays.stream(tStressInternal).reduce(0, (i, j) -> i + j) * 1.0 / Arrays.stream(countStressInternal).reduce(0, (i, j) -> i + j)
        return 0.0
      case TimingTask.COMM =>
        return tComm * 1.0 / countComm
      case TimingTask.STRESS_MERGE =>
        return tBCMerge * 1.0 / countBCMerge
      case TimingTask.STRESS_EXTRACT =>
        return tBCExtract * 1.0 / countBCExtract
    }
    return 0.0
  }
}
