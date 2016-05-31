package edu.indiana.soic.spidal.spark.damds.timing

import java.util.OptionalLong

import com.google.common.base.Stopwatch
import java.util.concurrent.TimeUnit
/**
  * Created by pulasthi on 5/28/16.
  */
object StressInternalTimings {

  object TimingTask extends Enumeration {
    type TimingTask = Value
    val COMP = Value
  }

  private var numThreads: Int = 0

  def init(numThreads: Int) {
    timerComp = new Array[Stopwatch](numThreads)
    for(i <- 0 until numThreads){
      timerComp(i) = Stopwatch.createUnstarted()
    }
    tComp = new Array[Long](numThreads)
    countComp = new Array[Long](numThreads)
    StressInternalTimings.numThreads = numThreads
  }

  private var timerComp: Array[Stopwatch] = null
  private var tComp: Array[Long] = null
  private var countComp: Array[Long] = null

  def startTiming(task: TimingTask.TimingTask, threadIdx: Int) {
    task match {
      case TimingTask.COMP =>
        timerComp(threadIdx).start
        countComp(threadIdx) += 1
    }
  }

  def endTiming(task: TimingTask.TimingTask, threadIdx: Int) {
    task match {
      case TimingTask.COMP =>
        timerComp(threadIdx).stop
        tComp(threadIdx) += timerComp(threadIdx).elapsed(TimeUnit.MILLISECONDS)
        timerComp(threadIdx).reset
    }
  }

//  def getTotalTime(task: TimingTask.TimingTask): Double = {
//    task match {
//      case TimingTask.COMP =>
//        val maxBofZ: OptionalLong = tComp.max
//        return if (maxBofZ.isPresent) maxBofZ.getAsLong * 1.0 else 0.0
//    }
//    return 0.0
//  }
//
//  def getAverageTime(task: TimingTask.TimingTask): Double = {
//    task match {
//      case TimingTask.COMP =>
//        return Arrays.stream(tComp).reduce(0, (i, j) -> i + j) * 1.0 / Arrays.stream(countComp).reduce(0, (i, j) -> i + j)
//    }
//    return 0.0
//  }
}
