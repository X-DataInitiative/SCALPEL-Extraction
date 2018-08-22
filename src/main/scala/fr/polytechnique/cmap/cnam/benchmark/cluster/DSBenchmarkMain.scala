package fr.polytechnique.cmap.cnam.benchmark.cluster

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.benchmark.BenchmarkBase
import fr.polytechnique.cmap.cnam.benchmark.functions.SimpleFunctions

object DSBenchmarkMain extends Main with BenchmarkBase {
  override def appName: String = "DS Benchmark"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {

    val numIters = argsMap.getOrElse("numIters", "100").toInt
    val numWarmUps = argsMap.getOrElse("numWarmUps", "10").toInt
    val numOfCols = argsMap.getOrElse("numOfCols", "100000").toInt
    val patientPath = argsMap("patientPath")

    val fMap = Map.empty[String, () => Unit] + (
      "DS map with scalar variable" -> { () => SimpleFunctions.datasetMapWithScalarVariable(sqlContext.sparkSession, numOfCols) },
      "DS map With primitive array" -> { () => SimpleFunctions.datasetMapWithPrimitiveArray(sqlContext.sparkSession, numOfCols) },
      "DS map with random data in 4 column" -> { () => SimpleFunctions.datasetWithRandomDataIn4Columns(sqlContext.sparkSession, numOfCols) },
      "DS load patient datas and then filter, sort" -> { () => SimpleFunctions.datasetWithPatientData(sqlContext.sparkSession, patientPath) }
    )
    runBenchmarkWithCases("DS Benchmark with 4 test operations", numIters, numWarmUps)(fMap)

    None
  }
}

