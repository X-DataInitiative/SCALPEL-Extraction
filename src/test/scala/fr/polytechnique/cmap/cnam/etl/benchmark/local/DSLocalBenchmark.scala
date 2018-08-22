package fr.polytechnique.cmap.cnam.etl.benchmark.local

import fr.polytechnique.cmap.cnam.benchmark.functions.SimpleFunctions

class DSLocalBenchmark extends LocalSparkBenchmarkBase {

  "DS Benchmark" should "run in local" in {
    val fMap = Map.empty[String, () => Unit] + (
      "DS map with scalar variable" -> { () => SimpleFunctions.datasetMapWithScalarVariable(spark, numOfCols) },
      "DS map With primitive array" -> { () => SimpleFunctions.datasetMapWithPrimitiveArray(spark, numOfCols) },
      "DS map with random data in 4 column" -> { () => SimpleFunctions.datasetWithRandomDataIn4Columns(spark, numOfCols) }
    )
    runBenchmarkWithCases("DF Benchmark in local", 100)(fMap)
  }

}
