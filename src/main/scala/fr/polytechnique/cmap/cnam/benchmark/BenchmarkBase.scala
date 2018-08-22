package fr.polytechnique.cmap.cnam.benchmark

trait BenchmarkBase {

  def runBenchmarkWithOneCase(
    name: String, numIters: Int = 10, numWarmUps: Int = 10, isNeedToOutputWarmUpTime: Boolean = false,
    isNeedToOutputIterationTime: Boolean = false)(caseName: String, f: => Unit): Unit = {
    Benchmark(name, numIters, numWarmUps, isNeedToOutputIterationTime, isNeedToOutputWarmUpTime)
      .addCase(caseName)(f)
      .runBenchmark()
  }

  def runBenchmarkWithCases(
    name: String, numIters: Int = 10, numWarmUps: Int = 10, isNeedToOutputWarmUpTime: Boolean = false,
    isNeedToOutputIterationTime: Boolean = false)(fMap: Map[String, () => Unit]): Unit = {
    fMap.foldLeft(Benchmark(name, numIters, numWarmUps, isNeedToOutputWarmUpTime, isNeedToOutputIterationTime)) {
      case (benchmark, (caseName, f)) => benchmark.addCase(caseName)(f())
    }.runBenchmark()
  }

}
