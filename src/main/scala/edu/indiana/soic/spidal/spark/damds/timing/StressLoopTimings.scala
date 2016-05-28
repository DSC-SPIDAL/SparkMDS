package edu.indiana.soic.spidal.spark.damds.timing

import com.google.common.base.Stopwatch
import java.util.concurrent.TimeUnit

/**
  * Created by pulasthi on 5/27/16.
  */
object StressLoopTimings {

  object TimingTask extends Enumeration {
    type TimingTask = Value
    val BC, CG, STRESS = Value
  }

  private var timerBC: Stopwatch = Stopwatch.createUnstarted
  private var timerCG: Stopwatch = Stopwatch.createUnstarted
  private var timerStress: Stopwatch = Stopwatch.createUnstarted

  private var tBC: Long = 0L
  private var tCG: Long = 0L
  private var tStress: Long = 0L
  private var countBC: Long = 0L
  private var countCG: Long = 0L
  private var countStress: Long = 0L

  def startTiming(task: TimingTask.TimingTask) {
    task match {
      case TimingTask.BC =>
        timerBC.start
        countBC += 1
      case TimingTask.CG =>
        timerCG.start
        countCG += 1
      case TimingTask.STRESS =>
        timerStress.start
        countStress += 1
    }
  }

  def endTiming(task: TimingTask.TimingTask) {
    task match {
      case TimingTask.BC =>
        timerBC.stop
        tBC += timerBC.elapsed(TimeUnit.MILLISECONDS)
        timerBC.reset
      case TimingTask.CG =>
        timerCG.stop
        tCG += timerCG.elapsed(TimeUnit.MILLISECONDS)
        timerCG.reset
      case TimingTask.STRESS =>
        timerStress.stop
        tStress += timerStress.elapsed(TimeUnit.MILLISECONDS)
        timerStress.reset
    }
  }

  def getTotalTime(task: TimingTask.TimingTask): Double = {
    task match {
      case TimingTask.BC =>
        return tBC
      case TimingTask.CG =>
        return tCG
      case TimingTask.STRESS =>
        return tStress
    }
    return 0.0
  }

  def getAverageTime(task: TimingTask.TimingTask): Double = {
    task match {
      case TimingTask.BC =>
        return tBC * 1.0 / countBC
      case TimingTask.CG =>
        return tCG * 1.0 / countCG
      case TimingTask.STRESS =>
        return tStress * 1.0 / countStress
    }
    return 0.0
  }

}
