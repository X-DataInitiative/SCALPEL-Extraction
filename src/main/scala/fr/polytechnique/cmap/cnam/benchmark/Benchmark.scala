package fr.polytechnique.cmap.cnam.benchmark

import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger

private[polytechnique] class Benchmark(
  name: String,
  numIters: Int = 10,
  numWarmUps: Int = 10,
  isNeedToOutputWarmUpTime: Boolean = true,
  isNeedToOutputIterationTime: Boolean = true) {

  import Benchmark._

  private val logger: Logger = Logger.getLogger(getClass)

  private val cases = ListBuffer.empty[Case]

  private def addTimeCase(name: String, overrideNumIters: Int = numIters)(f: Timer => Unit): Benchmark = {
    cases += Case(name, f, overrideNumIters)
    this
  }


  def addCase(name: String, overrideNumIters: Int = numIters)(f: => Unit): Benchmark =
    addTimeCase(name, overrideNumIters)(
      timer => {
        timer.start()
        f
        timer.stop()
      })

  private def measure(numIters: Int)(f: Timer => Unit): Result = {
    System.gc() // ensures garbage from previous cases don't impact this one
    (1 to numWarmUps).foreach(i => {
      val t = new Timer()
      f(t)
      if (isNeedToOutputWarmUpTime) logger.info(s"Warm up $i : ${t.totalTime / 1000000.0}ms")
    })

    val runTimes = (1 to numIters).map(i => {
      val t = new Timer()
      f(t)
      val iterTime = t.totalTime
      if (isNeedToOutputIterationTime) logger.info(s"Iteration $i : ${iterTime / 1000000.0}ms")
      iterTime
    })
    Result(runTimes.mean / 1000000.0, runTimes.min.toDouble / 1000000.0,
      runTimes.max.toDouble / 1000000.0, runTimes.standardDeviation / 1000000.0)
  }

  def runBenchmark(): Unit = {
    logger.info(s"running benchmark $name")
    val results = cases.map { c =>
      logger.info(s"running case ${c.name}")
      measure(c.numIters)(c.f)
    }

    logger.info("%-40s %16s %16s %16s %16s\n"
      .format(name + ":", "Avg Time(ms)", "Best Time(ms)", "Worst Time(ms),", "StDev(ms)"))
    logger.info("-" * 200)
    results.zip(cases).foreach { case (result, benchmark) =>
      logger.info("%-40s %16s %16s %16s %16s\n".format(
        benchmark.name,
        "%5.2f" format result.avg,
        "%5.2f" format result.best,
        "%5.2f" format result.worst,
        "%5.2f" format result.stdev
      ))
    }

  }

}

private[polytechnique] object Benchmark {

  def apply(
    name: String,
    numIters: Int = 10,
    numWarmUps: Int = 10,
    isNeedToOutputWarmUpTime: Boolean = true,
    isNeedToOutputIterationTime: Boolean = true): Benchmark =
    new Benchmark(name, numIters, numWarmUps, isNeedToOutputWarmUpTime, isNeedToOutputIterationTime)

  class Timer() {
    private var startTime: Long = 0L
    private var accumulatedTime = 0L

    def start(): Unit = {
      assert(startTime == 0L, "already started")
      startTime = System.nanoTime()
    }

    def stop(): Unit = {
      assert(startTime != 0, "not yet started")
      accumulatedTime += System.nanoTime() - startTime
      startTime = 0L
    }

    def totalTime: Long = {
      assert(startTime == 0, "not yet stopped")
      accumulatedTime
    }
  }

  case class Case(name: String, f: Timer => Unit, numIters: Int = 10)

  case class Result(avg: Double, best: Double, worst: Double, stdev: Double)

  implicit class SeqWithStatistic[A](seq: Seq[A]) {
    def mean(implicit num: Numeric[A]): Double = num.toDouble(seq.sum) / seq.size

    def standardDeviation(implicit num: Numeric[A]): Double = {
      val avg = mean
      val variance = seq.map(num.toDouble).map(x => math.pow(x - avg, 2)).sum / seq.size
      math.sqrt(variance)
    }

  }

}
