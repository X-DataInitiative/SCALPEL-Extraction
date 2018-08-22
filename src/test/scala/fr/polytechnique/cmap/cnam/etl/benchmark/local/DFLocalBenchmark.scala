package fr.polytechnique.cmap.cnam.etl.benchmark.local

import fr.polytechnique.cmap.cnam.benchmark.functions.SimpleFunctions

class DFLocalBenchmark extends LocalSparkBenchmarkBase {


  "DF Benchmark" should "run in local" in {
    val fMap = Map.empty[String, () => Unit] + (
      "DF map with scalar variable" -> { () => SimpleFunctions.dataframeMapWithScalarVariable(spark, numOfCols) },
      "DF map With primitive array" -> { () => SimpleFunctions.dataframeMapWithPrimitiveArray(spark, numOfCols) },
      "DF map with random data in 4 column" -> { () => SimpleFunctions.dataframeWithRandomDataIn4Columns(spark, numOfCols) }
    )
    runBenchmarkWithCases("DF Benchmark in local", 100)(fMap)
  }

}
