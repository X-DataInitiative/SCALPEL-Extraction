package fr.polytechnique.cmap.cnam.util

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.RichDataFrame._


private case class Alpha(name: String, param: Int=2)
private case class Beta(name: String, age: Int)

class RichDataFrameSuite extends SharedContext {

  "===" should "return true" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1,2,3)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2)).toDF("toto")

    // When
    val result = df1 === df2

    // Then
    assert(result)
  }

  it should "return false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1,2,4)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2)).toDF("toto")

    // When
    val result = df1 === df2

    // Then
    assert(!result)
  }

  it should "return false when inconsistent duplicates are found" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1,2,3,2)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2,3)).toDF("toto")

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
}
