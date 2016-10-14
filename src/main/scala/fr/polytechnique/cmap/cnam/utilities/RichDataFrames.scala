package fr.polytechnique.cmap.cnam.utilities


import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by burq on 30/06/16.
  */
object RichDataFrames {

  implicit class CSVDataFrame(dataFrame: DataFrame) {

    override def toString: String = dataFrame.toString

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
}
