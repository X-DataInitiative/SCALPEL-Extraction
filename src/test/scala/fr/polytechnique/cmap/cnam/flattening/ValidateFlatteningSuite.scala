package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpecLike
import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 21/09/16.
  */
class ValidateFlatteningSuite extends SharedContext {


  "cleanDfColumnNames" should "replace the . delimiter in the column names by _" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf: DataFrame = Seq(
      ("Column1", 7, 8, 7),
      ("Column2", 3, 4, 5)
    ).toDF("column1", "column.2", "column.3", "column4")

    val expected: DataFrame = Seq(
      ("Column1", 7, 8, 7),
      ("Column2", 3, 4, 5)
    ).toDF("column1", "column_2", "column_3", "column4")

    // When
    import ValidateFlattening._
    val outputDf = inputDf.cleanDFColumnNames

    // Then
    import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
    assert(outputDf === expected)
  }

  "prefixColumnNameWithDelimiter" should "prefix a text in front of each column name " +
    "with _ at the end" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf: DataFrame = Seq(
      ("Column1", 9, 8, 7),
      ("Column2", 3, 4, 5)
    ).toDF("column1", "column2", "column3", "column4")

    val expected: DataFrame = Seq(
      ("Column1", 9, 8, 7),
      ("Column2", 3, 4, 5)
    ).toDF("anyText_column1", "anyText_column2", "anyText_column3", "anyText_column4")

    // When
    import ValidateFlattening._
    val outputDf = inputDf.prefixColumnNames("anyText")

    // Then
    import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
    assert(outputDf === expected)
  }

  it should("not prefix the given text to the ignore columns") in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf: DataFrame = Seq(
      ("Column1", 9, 8, 7),
      ("Column2", 3, 4, 5)
    ).toDF("column1", "column2", "column3", "column4")

    val expected: DataFrame = Seq(
      ("Column1", 9, 8, 7),
      ("Column2", 3, 4, 5)
    ).toDF("column1", "anyText_column2", "anyText_column3", "column4")

    // When
    import ValidateFlattening._
    val outputDf = inputDf.prefixColumnNames("anyText", Seq("column1", "column4"))

    // Then
    import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
    outputDf.printSchema
    expected.printSchema
    assert(outputDf === expected)
  }

  "validateFlattening" should "modify the column names of flat and individual DFs " +
    "and write the computed statistics under joins folder" in {

    println("validateFlattening test suite is disabled as it would take lot of time to pass")
    //ValidateFlattening.main(Array[String]())
  }
}
