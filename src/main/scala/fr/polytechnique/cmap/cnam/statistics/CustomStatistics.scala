package fr.polytechnique.cmap.cnam.statistics

import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by sathiya on 26/07/16.
  */

/**
  * This case class represents needed (statistics) columns for the output dataframe.
  *
  * @param Min
  * @param Max
  * @param Count
  * @param Avg
  * @param Sum
  * @param MaxOccur
  * @param MinOccur
  * @param ColName
  */
case class CaseDescribeFunctionColumns(Min: String,
                                       Max: String,
                                       Count: Long,
                                       Avg: String,
                                       Sum: String,
                                       MaxOccur: String,
                                       MinOccur: String,
                                       ColName: String)

object CustomStatistics {

  /** WE MAY NEED TO PUT THIS FUNCTION INSIDE UTILITIES/ MAIN LATER.
    * Only one SparkContext can exists for a given application.
    * This method helps not to recreate a new Spark Context if one already exists.
    *
    */
//  def getSparkContext(): SparkContext = {
//
//    val sparkConf = new SparkConf().setAppName("Jira Id CNAM 41")
//
//    SparkContext.getOrCreate(sparkConf)
//  }

  implicit class Statistics(val df: DataFrame) {

    val sqlContext = df.sqlContext
    val sc = sqlContext.sparkContext
    import sqlContext.implicits._

    def customDescribe(columnNames: String*): DataFrame = {

      val inputColumns: List[String] = {
        if (columnNames.isEmpty) df.columns.toList
        else columnNames.toList
      }

      def computeAvailableAgg(schema: StructType,
                              colName: String): DataFrame =
        colName match {
            // todo if-then-else ?
        case colName if schema.apply(colName).dataType.isInstanceOf[NumericType] =>
          df.select(colName)
                  .filter(!col(colName).isin(""))
                  .agg(
                    min(colName),
                    max(colName),
                    count(colName),
                    avg(colName),
                    sum(colName)
                  ).withColumn("ColName", lit(colName))

        case colName =>
          df.select(colName)
            .filter(!col(colName).isin(""))
            .agg(
              min(colName),
              max(colName),
              count(colName)
            ).withColumn("Avg", lit("NA"))
            .withColumn("Sum", lit("NA"))
            .withColumn("ColName", lit(colName))
      }

      val outputDF: DataFrame = inputColumns
        .map(computeAvailableAgg(df.schema, _))
        .reduce(_.unionAll(_))

      val maxOccur: Map[String, String] = inputColumns
        .map{
          name =>
            val mostOccurrent: String = StatisticsUsingMapReduce
              .findMostOccurrenceValues(df, name)
              .mkString

            (name, mostOccurrent)
        }.toMap

      val minOccur: Map[String, String] = inputColumns.map{
        name =>
          val leastOccurent: String = StatisticsUsingMapReduce
            .findLeastOccurrenceValues(df, name)
            .mkString

          (name, leastOccurent)
      }.toMap

      outputDF.map((r: Row) => CaseDescribeFunctionColumns(r.getString(0),
        r.getString(1),
        r.getLong(2),
        r.getString(3),
        r.getString(4),
        maxOccur.get(r.getString(5)).get,
        minOccur.get(r.getString(5)).get,
        r.getString(5)
      )).toDF()
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
        .reduce(_.unionAll(_))
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
