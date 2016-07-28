package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by sathiya on 26/07/16.
  */
object StatisticsUsingMapReduce {

  /**
    * This method takes a dataframe and a column name as input and computes median for the given column name.
    *
    * This method filters out the empty columns, sort the values by ascending order and convet the df to rdd.
    * With the help of the zipWithIndex method, the median is calculated:
    * val median = (df.count %2 == 0) ? (average of the two middle values) else (value at the middle position)
    *
    * @param srcDF
    * @param colName Name of the column on which the median has to be computed.
    * @return
    */
  def findMedian(srcDF: DataFrame, colName: String): Double = {

    // 'filter(! srcDF(colName).isin(""))' is used to remove empty columns.
    // If we are switching to scala 2.11, we can use spar_csv 2.11 and consider empty columns as null
    val sortedAsRdd = srcDF.select(colName).filter(!srcDF(colName).isin("")).sort(asc(colName)).map(x => x.get(0))

    val sortedValuesAsKey = sortedAsRdd.zipWithIndex().map(_.swap)

    val count = sortedValuesAsKey.count()

    def sortedValuesAsKeyAtPosition(x: Long): Double = return sortedValuesAsKey.lookup(x).head.toString.toDouble

    val median: Double = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      // [Quick fix, this may not be the cleaner way] String can be casted to any other types,
      // so first cast the type to string and then to needed double.
      (sortedValuesAsKeyAtPosition(l) + sortedValuesAsKeyAtPosition(r)) / 2
    } else sortedValuesAsKeyAtPosition(count/2)

    median
  }

  /**
    * This method does groupBy on a given column name of a df and counts the number of occurrence of each value.
    *
    * @param srcDF
    * @param colName
    * @return a dataframe with the count as the first column and the actual value as the second column.
    */
  def mapNbOccurrencesToColValues(srcDF: DataFrame, colName: String): RDD[(Long, Any)] = {

    val mappedrdd = { srcDF.select(colName)
      .filter((srcDF(colName).isNotNull) && not(col(colName).isin("")))
      .groupBy(colName)
      .count()
      .map(x => (x.getLong(1), x.get(0)))
    }

    mappedrdd
  }

  /**
    * The method sorts (descending) the number of occurrence of each value in a given column.
    *
    * @param srcDF
    * @param colName
    * @param nb number of top most occurrence values to return (by default 1).
    * @return first nb rows in the sorted dataframe.
    */
  def findMostOccurrenceValues(srcDF: DataFrame, colName: String, nb: Int = 1): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(srcDF, colName).sortByKey(false) // false => sort descending (Most Occurrences)

    mappedRDD.take(nb).map(_.swap)
  }

  /**
    * Same as the previous method, but instead of sorting descending, this will sort ascending
    *
    * @param srcDF
    * @param colName
    * @param nb number of <i>least</i> occurrence values to return (by default 1).
    * @return
    */
  def findLeastOccurrenceValues(srcDF: DataFrame, colName: String, nb: Int = 1): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(srcDF, colName).sortByKey(true) // true => sort ascending (Least Occurrences)

    mappedRDD.take(nb).map(_.swap)
  }

}