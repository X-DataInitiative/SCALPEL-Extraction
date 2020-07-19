// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}


class RichDataFrame(dataFrame: DataFrame) {

  /**
    * This method compares the equality of two data frames. To qualify equality, the rows
    * can be in different order but the columns should be in the right order.
    */
  //TODO: This implementation may not be efficient, we should use Karau method from this link:
  // https://github.com/holdenk/spark-testing-base/blob/master/src/main/pre-2.0/scala/com/holdenkarau/spark/testing/DataFrameSuiteBase.scala
  def ===(other: DataFrame): Boolean = {

    def checkDuplicateRows: Boolean = {
      val dataFrameGroupedByRows = dataFrame.groupBy(
        dataFrame.columns.head,
        dataFrame.columns.tail: _*
      ).count()
      val otherGroupedByRows = other.groupBy(
        other.columns.head,
        other.columns.tail: _*
      ).count()

      dataFrameGroupedByRows.except(otherGroupedByRows).count() == 0 &&
        otherGroupedByRows.except(dataFrameGroupedByRows).count == 0
    }

    def columnNameType(schema: StructType): Seq[(String, DataType)] = {
      schema.fields.map((field: StructField) => (field.name, field.dataType))
    }

    columnNameType(dataFrame.schema) == columnNameType(other.schema) &&
      checkDuplicateRows
  }

  def writeCSV(path: String, mode: String = "errorIfExists"): Unit = {
    dataFrame.coalesce(1)
      .write
      .mode(saveMode(mode))
      .option("delimiter", ",")
      .option("header", "true")
      .csv(path)
  }

  def writeParquet(path: String, mode: String = "errorIfExists"): Unit = {
    dataFrame.write
      .mode(saveMode(mode))
      .parquet(path)
  }

  def writeOrc(path: String, mode: String = "errorIfExists"): Unit = {
    dataFrame.write
      .mode(saveMode(mode))
      .orc(path)
  }

  def write(path: String, mode: String = "errorIfExists", format: String = "parquet"): Unit = {
    format match {
      case "orc" => writeOrc(path, mode)
      case "csv" => writeCSV(path, mode)
      case _ => writeParquet(path, mode)
    }
  }


  private def saveMode(mode: String): SaveMode = mode match {
    case "overwrite" => SaveMode.Overwrite
    case "append" => SaveMode.Append
    case "errorIfExists" => SaveMode.ErrorIfExists
    case "withTimestamp" => SaveMode.Overwrite
  }

  def avoidSpecialCharactersBeforePivot(column: String): DataFrame = {
    //replace ",; " to "_"
    val underscoreCol = regexp_replace(col(column), "[\\,,;, ]", "_")
    //replace "(){}" to ""
    val bracketCol = regexp_replace(underscoreCol, "[\\(,\\),\\{,\\}]", "")
    dataFrame.withColumn(column, bracketCol)
  }
}

object RichDataFrame {

  implicit def toRichDataFrame(dataFrame: DataFrame): RichDataFrame = new RichDataFrame(dataFrame)

  def renameTupleColumns[A: TypeTag, B: TypeTag](input: Dataset[(A, B)]): Dataset[(A, B)] = {
    import input.sqlContext.implicits._
    val aName = typeOf[A].typeSymbol.name.toString
    val bName = typeOf[B].typeSymbol.name.toString
    input.withColumnRenamed("_1", aName).withColumnRenamed("_2", bName).as[(A, B)]
  }

}
