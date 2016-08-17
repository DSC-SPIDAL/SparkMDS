package edu.indiana.soic.spidal.spark.damds.timing


import com.google.common.base.Stopwatch
import java.util.concurrent.TimeUnit

/**
  * Created by pulasthi on 5/27/16.
  */
object  TemperatureLoopTimings {

  object TimingTask extends Enumeration {
    type TimingTask = Value
    val PRE_STRESS, STRESS_LOOP = Value
  }

  private var timerPreStress: Stopwatch = new Stopwatch();
  private var timerStressLoop: Stopwatch = new Stopwatch();
  private var tPreStress: Long = 0L
  private var tStressLoop: Long = 0L
  private var countPreStress: Long = 0L
  private var countStressLoop: Long = 0L

  def startTiming(task: TimingTask.TimingTask) {
    task match {
      case TimingTask.PRE_STRESS =>
        timerPreStress.start
        countPreStress += 1
      case TimingTask.STRESS_LOOP =>
        timerStressLoop.start
        countStressLoop += 1
    }
  }

  def endTiming(task: TimingTask.TimingTask) {
    task match {
      case TimingTask.PRE_STRESS =>
        timerPreStress.stop
        tPreStress += timerPreStress.elapsed(TimeUnit.MILLISECONDS)
        timerPreStress.reset
      case TimingTask.STRESS_LOOP =>
        timerStressLoop.stop
        tStressLoop += timerStressLoop.elapsed(TimeUnit.MILLISECONDS)
        timerStressLoop.reset
    }
  }

  def getTotalTime(task: TimingTask.TimingTask): Double = {
    task match {
      case TimingTask.PRE_STRESS =>
        return tPreStress
      case TimingTask.STRESS_LOOP =>
        return tStressLoop
    }
    return 0.0
  }

  def getAverageTime(task: TimingTask.TimingTask): Double = {
    task match {
      case TimingTask.PRE_STRESS =>
        return tPreStress * 1.0 / countPreStress
      case TimingTask.STRESS_LOOP =>
        return tStressLoop * 1.0 / countStressLoop
    }
    return 0.0
  }
}
