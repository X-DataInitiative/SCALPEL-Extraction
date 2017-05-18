package fr.polytechnique.cmap.cnam.util

import scala.reflect.runtime.universe.{typeOf, TypeTag}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}


object RichDataFrames {

  implicit class CSVDataFrame(dataFrame: DataFrame) {

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

  }

  def renameTupleColumns[A: TypeTag, B: TypeTag](input: Dataset[(A, B)]): Dataset[(A, B)] = {
    import input.sqlContext.implicits._
    val aName = typeOf[A].toString().split('.').last
    val bName = typeOf[B].toString().split('.').last
    input.withColumnRenamed("_1", aName).withColumnRenamed("_2", bName).as[(A,B)]
  }
}
