package fr.polytechnique.cmap.cnam.stats

import scala.reflect.ClassTag
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by sathiya on 26/07/16.
  */

object CustomStatistics {

  implicit class Statistics(val df: DataFrame) {

    val sqlContext = df.sqlContext
    val sc = sqlContext.sparkContext
    import sqlContext.implicits._

    // In order to preserve old API calls (the one without boolean parameter) to
    // customDescribe method, i am doing this. It is open to debate.
    def customDescribe(): DataFrame = customDescribe(true, df.columns)

    def customDescribe(forComparison: Boolean): DataFrame =
      customDescribe(forComparison, df.columns)

    def customDescribe(otherColumns: String*): DataFrame =
      customDescribe(true, otherColumns.toArray)

    def customDescribe(forComparison: Boolean,
                       otherColumns: String*): DataFrame =
      customDescribe(forComparison, otherColumns.toArray)

    // To compare flatDf columns to columns from individual df's, we need to compare only
    // statistics on distinct values. Inorder to do this without any code duplication,
    // i am adding a boolean parameter to this function.
    private def customDescribe(forComparison: Boolean,
                               inputColumns: Array[String]): DataFrame = {

      def computeAvailableAgg(schema: StructType,
                              colName: String): DataFrame =
        colName match {
            // todo if-then-else ?
        case colName if schema.apply(colName).dataType.isInstanceOf[NumericType] =>
          df.select(colName)
                  .filter(!col(colName).isin(""))
                  .agg(
                    min(colName) cast("string") as "Min",
                    max(colName) cast("string") as "Max",
                    count(colName) cast("long") as "Count",
                    countDistinct(colName) cast("long") as "CountDistinct",
                    sum(colName) cast("string") as "Sum",
                    sumDistinct(colName) cast("string") as "SumDistinct",
                    avg(colName) cast("string") as "Avg" // "AvgDistinct" metric removed after GC overloaded problem
                  ).withColumn("ColName", lit(colName))

        case colName =>
          df.select(colName)
                  .filter(!col(colName).isin(""))
                  .agg(
                    min(colName) cast("string") as "Min",
                    max(colName) cast("string") as "Max",
                    count(colName) cast("long") as "Count",
                    countDistinct(colName) cast("long") as "CountDistinct"
                  ).withColumn("Sum", lit("NA"))
                  .withColumn("SumDistinct", lit("NA"))
                  .withColumn("Avg", lit("NA")) // "AvgDistinct" metric removed after GC overloaded problem
                  .withColumn("ColName", lit(colName))
      }

      val outputDF: DataFrame = inputColumns
        .map(computeAvailableAgg(df.schema, _))
        .reduce(_.union(_))

      if(forComparison == true)
      {
        outputDF
          .drop("Count")
          .drop("Sum")
          .drop("Avg")
      }
      else
      {
        val maxOccur: Map[String, String] = inputColumns
          .map{
            name =>
              val mostOccurrence: String = StatisticsUsingMapReduce
                .findMostOccurrenceValues(df, name)
                .mkString

              (name, mostOccurrence)
          }.toMap

        val minOccur: Map[String, String] = inputColumns.map{
          name =>
            val leastOccurrence: String = StatisticsUsingMapReduce
              .findLeastOccurrenceValues(df, name)
              .mkString

            (name, leastOccurrence)
        }.toMap

        val addMaxOccur = udf({x:String => maxOccur(x)})
        val addMinOccur = udf({(x:String) => minOccur(x)})

        outputDF
          .withColumn("MinOccur", addMinOccur($"ColName"))
          .withColumn("MaxOccur", addMaxOccur($"ColName"))
          .select("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct",
                  "Avg", "MaxOccur", "MinOccur", "ColName")
      }
    }

    /**
      * This is a centralized function to compute any UDAF and return a dataframe
      * that is composed of the computed metric and the column name.
      *
      * @param udafObject object of the UDAF that we would like to compute.
      * @param resultColumnName column name for the computed metric.
      * @param columnNames set of column names on which the UDAF should be applied.
      */
    private def computeUDAF(udafObject: UserDefinedAggregateFunction,
                            resultColumnName: String,
                            columnNames: Seq[String]): DataFrame = {

      val inputColumns = {
        if (columnNames.isEmpty) df.columns.toList
        else columnNames.toList
      }

      def computeUDAF(colName: String): DataFrame = {
        df.select(colName)
                .agg(udafObject(df(colName)))
                .withColumn("ColName", lit(colName))
      }

      inputColumns
        .map(computeUDAF(_))
        .reduce(_.union(_))
    }

    /** This function counts number of unexpected values in a column <i> using UDAF </i>
      *
      * @param expectedValues set of expected values
      * @param inputColumns names of the input columns that have the same expected values
      * @tparam T
      */
    def countUnexpectedValuesUsingUDAF[T: ClassTag](expectedValues: Set[T],
                                                    inputColumns: String*): DataFrame = {

      val udaf = new UDAFUnexpectedValues(expectedValues)
      val columnName = "Count_Unexpected"

      computeUDAF(udaf, columnName, inputColumns)
    }

    /** Same as the previous function, but counts expected values instead of
      * unexpected values <i> using UDAF </i>
      *
      * @param expectedValues
      * @param inputColumns
      * @tparam T
      */
    def countExpectedValuesUsingUDAF[T: ClassTag](expectedValues: Set[T],
                                                  inputColumns: String*): DataFrame = {

      val udaf = new UDAFExpectedValues(expectedValues)
      val columnName = "Count_Expected"

      computeUDAF(udaf, columnName, inputColumns)
    }

    /** This is centralized function for the following two functions that does same as
      * the previous two, but <i>without</i> UDAF
      * After performance comparison, we can deprecate the method that is slow and
      * can be eventually removed.
      *
      * @param colName
      * @param expectedValues
      * @param isExpected
      * @tparam T
      */
    private def countExpectedOrUnexpectedValues[T: ClassTag](colName: String,
                                                             expectedValues: Set[T],
                                                             isExpected: Boolean): DataFrame = {

      val castedExpectedValues = expectedValues.map(_.toString).toList
      val isInExpectedValues = {
        df.col(colName)
        .cast(StringType)
        .isin(castedExpectedValues: _*)
      }// Casting to IntegerType should not be done here.

      val columnToAggregate = {
        if (isExpected) isInExpectedValues.cast(IntegerType)
        else not(isInExpectedValues).cast(IntegerType)
      }

      val outputColName: String = if (isExpected) "ExpectedValues" else "UnexpectedValues"

      df.filter((col(colName).isNotNull) && not(col(colName).isin("")))
              .select(colName)
              .agg(sum(columnToAggregate))
              .toDF(outputColName)
              .withColumn("ColName", lit(colName))
    }

    def countUnexpectedValues[T: ClassTag](colName: String,
                                           expectedValues: Set[T]): DataFrame = {

      countExpectedOrUnexpectedValues(colName, expectedValues, false)
    }

    def countExpectedValues[T: ClassTag](colName: String,
                                         expectedValues: Set[T]): DataFrame = {

      countExpectedOrUnexpectedValues(colName, expectedValues, true)
    }
  }
}
