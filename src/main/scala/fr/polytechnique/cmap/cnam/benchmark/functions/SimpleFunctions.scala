package fr.polytechnique.cmap.cnam.benchmark.functions

import scala.util.Random._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.patients.Patient

object SimpleFunctions {

  case class Dummy(v1: Long, v2: Long, v3: String, v4: String) extends java.io.Serializable with java.lang.Cloneable

  def dataframeMapWithScalarVariable(spark: SparkSession, numOfCols: Int): Unit = {
    import spark.implicits._
    val df = (1 to numOfCols).toDF().cache()
    df.selectExpr("value + 1").collect()
  }

  def datasetMapWithScalarVariable(spark: SparkSession, numOfCols: Int): Unit = {
    import spark.implicits._
    val df = (1 to numOfCols).toDS().cache()
    df.map(_ + 1).collect()
  }

  def dataframeMapWithPrimitiveArray(spark: SparkSession, numOfCols: Int): Unit = {
    import spark.implicits._
    val df = (1 to numOfCols).map(Array(_)).toDF("a").cache()
    df.selectExpr("Array(a[0])").collect()
  }

  def datasetMapWithPrimitiveArray(spark: SparkSession, numOfCols: Int): Unit = {
    import spark.implicits._
    val df = (1 to numOfCols).map(Array(_)).toDS().cache()
    df.map(a => Array(a(0))).collect()
  }

  def dataframeWithRandomDataIn4Columns(spark: SparkSession, numOfCols: Int): Unit = {
    import spark.implicits._
    val df = spark.range(numOfCols.toLong).map(i => (nextInt(), nextLong(), nextDouble(), nextString(10)))
      .toDF("v1", "v2", "v3", "v4").cache()
    df.selectExpr("v1 + 1 as v1", "v2 + 2 as v2", "v3 + 3 as v3", "v4").collect()
  }

  def datasetWithRandomDataIn4Columns(spark: SparkSession, numOfCols: Int): Unit = {
    import spark.implicits._
    val df = spark.range(numOfCols.toLong).map(i => (nextInt(), nextLong(), nextDouble(), nextString(10)))
      .cache()
    df.select($"_1" as "v1", $"_2" as "v2", $"_3" as "v3", $"_4" as "v4").as[Dummy]
      .selectExpr("v1 + 1 as v1", "v2 + 2 as v2", "v3 + 3 as v3", "v4").collect()
  }

  def dataframeWithPatientData(spark: SparkSession, patientsPath: String): Unit = {

    val patients = spark.read.parquet(patientsPath).cache()
    patients
      .where(col("gender") === 1)
      .orderBy("birthDate")
      .collect()
  }

  def datasetWithPatientData(spark: SparkSession, patientsPath: String): Unit = {
    import spark.implicits._
    val patients = spark.read.parquet(patientsPath).as[Patient].cache()
    patients
      .where(col("gender") === 1)
      .orderBy("birthDate")
      .collect()
  }


}
