package fr.polytechnique.cmap.cnam.util

import scala.reflect.runtime.universe.{TypeTag, typeOf}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}


class RichDataFrame(dataFrame: DataFrame) {

  def writeParquet(path: String, isOverwrite: Boolean = true ): Unit = {
    val saveMode : SaveMode = if(isOverwrite) SaveMode.Ignore else SaveMode.Overwrite
    dataFrame.write
      .mode(saveMode)
      .parquet(path)
  }

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
        dataFrame.columns.tail: _*).count()
      val otherGroupedByRows = other.groupBy(
        other.columns.head,
        other.columns.tail: _*).count()

      dataFrameGroupedByRows.except(otherGroupedByRows).count() == 0 &&
        otherGroupedByRows.except(dataFrameGroupedByRows).count == 0
    }

    def columnNameType(schema: StructType): Seq[(String, DataType)] = {
      schema.fields.map((field: StructField) => (field.name, field.dataType))
    }

    columnNameType(dataFrame.schema) == columnNameType(other.schema) &&
      checkDuplicateRows
  }

  def writeCSV(path: String): Unit = {
    dataFrame.coalesce(1).write
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .save(path)
  }

  def withIndices(columnNames: Seq[String]): DataFrame = {
    import dataFrame.sqlContext.implicits._
    columnNames.foldLeft(dataFrame){
      (currentData, columnName) => {

        // WARNING: The following code is dangerous, but there is no perfect solution.
        //   It was tested with up to 30 million distinct values with 15 characters and 300
        //   million total rows. If we have more than that, we may need to fall back to a join
        //   option, which would take very much longer.
        val labels = currentData
          .select(col(columnName).cast(StringType))
          .distinct
          .map(_.getString(0))
          .collect
          .toSeq
          .sorted

        // It's transient to avoid serializing the full Map. Only the labels Seq will be
        // serialized and each executor will compute their own Map. This is slower but allows more
        // labels to be indexed.
        @transient lazy val indexerFuc = labels.zipWithIndex.toMap.apply _

        val indexer = udf(indexerFuc)

        currentData.withColumn(columnName + "Index", indexer(col(columnName)))
      }
    }
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
