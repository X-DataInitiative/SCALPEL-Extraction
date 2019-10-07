// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util

import org.apache.spark.sql.AnalysisException
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.RichDataFrame._


private case class Alpha(name: String, param: Int = 2)

private case class Beta(name: String, age: Int)

class RichDataFrameSuite extends SharedContext {

  "===" should "return true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1, 2, 3)).toDF("toto")
    val df2 = sc.parallelize(Seq(1, 3, 2)).toDF("toto")

    // When
    val result = df1 === df2

    // Then
    assert(result)
  }

  it should "return false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1, 2, 4)).toDF("toto")
    val df2 = sc.parallelize(Seq(1, 3, 2)).toDF("toto")

    // When
    val result = df1 === df2

    // Then
    assert(!result)
  }

  it should "return false when inconsistent duplicates are found" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1, 2, 3, 2)).toDF("toto")
    val df2 = sc.parallelize(Seq(1, 3, 2, 3)).toDF("toto")

    // When
    val result = df1 === df2

    // Then
    assert(!result)
  }

  "avoidSpecialCharactersBeforePivot" should "replace all the special characters in a column" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val df = Seq("ROS A", "INS (B)", "PIO,{C}").toDF("col1")

    val expected = Seq("ROS_A", "INS_B", "PIO_C").toDF("col1")

    //When
    val result = df.avoidSpecialCharactersBeforePivot("col1")

    //Then
    assertDFs(result, expected)
  }

  "renameDataset" should "rename the two columns of a dataset from tuple" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val ds = Seq(
      (Alpha("toto"), Beta("toto", 3)),
      (Alpha("gerard"), Beta("gerard", 18))
    ).toDS

    val expected = Seq(
      (Alpha("toto"), Beta("toto", 3)),
      (Alpha("gerard"), Beta("gerard", 18))
    ).toDS
      .withColumnRenamed("_1", "Alpha")
      .withColumnRenamed("_2", "Beta")
      .as[(Alpha, Beta)]

    // When
    val result = renameTupleColumns(ds)

    // Then
    assertDSs(result, expected)
  }

  "writeParquet" should "write the data correctly in a parquet with the different strategies" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = "target/test/output"
    val expected = data
    val expectedAppend = data.union(data)

    //When
    data.writeParquet(path, "overwrite")
    val result = spark.read.parquet(path)
    val exception = intercept[Exception] {
      data.writeParquet(path)
    }
    data.writeParquet(path, "append")
    val resultAppend = spark.read.parquet(path)
    data.writeParquet("target/test/dummy/output", "withTimestamp")
    val resultWithTimestamp = spark.read.parquet("target/test/dummy/output")

    //Then
    // Then
    assertDFs(result, expected)
    assert(exception.isInstanceOf[AnalysisException])
    assertDFs(resultAppend, expectedAppend)
    assertDFs(resultWithTimestamp, expected)
  }

  "writeCSV" should "write the data correctly in a CSV with the different strategies" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = "target/test/output.csv"
    val expected = data
    val expectedAppend = data.union(data)
    //When
    data.writeCSV(path, "overwrite")
    val result = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    val exception = intercept[Exception] {
      data.writeCSV(path)
    }
    data.writeCSV(path, "append")
    val resultAppend = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    data.writeCSV("target/test/dummy/output.csv", "withTimestamp")
    val resultWithTimestamp = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("target/test/dummy/output.csv")

    // Then
    assertDFs(result, expected)
    assert(exception.isInstanceOf[AnalysisException])
    assertDFs(resultAppend, expectedAppend)
    assertDFs(resultWithTimestamp, expected)
  }

}
