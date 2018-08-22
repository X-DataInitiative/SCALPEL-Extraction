package fr.polytechnique.cmap.cnam.benchmark.cluster

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.benchmark.BenchmarkBase
import fr.polytechnique.cmap.cnam.benchmark.functions.SimpleFunctions

object DFBenchmarkMain extends Main with BenchmarkBase {
  override def appName: String = "DF Benchmark"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {

    val numIters = argsMap.getOrElse("numIters", "100").toInt
    val numWarmUps = argsMap.getOrElse("numWarmUps", "10").toInt
    val numOfCols = argsMap.getOrElse("numOfCols", "100000").toInt
    val patientPath = argsMap("patientPath")

    val fMap = Map.empty[String, () => Unit] + (
      "DF map with scalar variable" -> { () => SimpleFunctions.dataframeMapWithScalarVariable(sqlContext.sparkSession, numOfCols) },
      "DF map With primitive array" -> { () => SimpleFunctions.dataframeMapWithPrimitiveArray(sqlContext.sparkSession, numOfCols) },
      "DF map with random data in 4 column" -> { () => SimpleFunctions.dataframeWithRandomDataIn4Columns(sqlContext.sparkSession, numOfCols) },
      "DF load patient datas and then filter, sort" -> { () => SimpleFunctions.dataframeWithPatientData(sqlContext.sparkSession, patientPath) }

    )
    runBenchmarkWithCases("DF Benchmark with 4 test operations", numIters, numWarmUps)(fMap)

    None
  }
}
