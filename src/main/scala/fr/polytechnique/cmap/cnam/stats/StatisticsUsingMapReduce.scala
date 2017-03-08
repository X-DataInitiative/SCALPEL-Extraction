package fr.polytechnique.cmap.cnam.stats

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by sathiya on 26/07/16.
  */
object StatisticsUsingMapReduce {

  /**
    * THIS FUNCTION IS CURRENTLY DEPRECATED.
    * IN FUTURE, WE MIGHT BE USING SOME APPROXIMATION METHOD.
    *
    * This method takes a dataframe and a column name as input and computes median
    * for the given column name.
    *
    * This method filters out the empty columns, sort the values by ascending order
    * and convet the df to rdd.
    * With the help of the zipWithIndex method, the median is calculated:
    * val median = (df.count %2 == 0) ? (average of the two middle values) else
    * (value at the middle position)
    *
    * @param sourceDF
    * @param colName Name of the column on which the median has to be computed.
    */
  // todo REMOVE THIS OR TEST IT
  def findMedian(sourceDF: DataFrame, colName: String): Double = {

    // 'filter(! srcDF(colName).isin(""))' is used to remove empty columns. If we are
    // switching to scala 2.11, we can use spar_csv 2.11 and consider empty columns as null
    val sortedAsRdd = {
      sourceDF
        .select(colName)
        .filter(!sourceDF(colName).isin(""))
        .sort(asc(colName))
        .rdd
        .map(x => x.get(0))
    }

    val sortedValuesAsKey = sortedAsRdd.zipWithIndex().map(_.swap)

    val count = sortedValuesAsKey.count()

    def sortedValuesAsKeyAtPosition(x: Long): Double = {
      sortedValuesAsKey.lookup(x).head.toString.toDouble
    }

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
    * This method does groupBy on a given column name of a DF and counts the number of
    * occurrence of each value and return a DF with the count as the first column
    * and the actual value as the second column.
    *
    * @param sourceDF
    * @param columnName
    */
  def mapNbOccurrencesToColValues(sourceDF: DataFrame,
                                  columnName: String): RDD[(Long, Any)] = {

    val mappedRDD = {
      sourceDF
        .select(columnName)
        .filter(sourceDF(columnName).isNotNull)
        .groupBy(columnName)
        .count()
        .rdd
        .map(x => (x.getLong(1), x.get(0)))
    }

    mappedRDD
  }

  /**
    * The method sorts (descending) the number of occurrence of each value in a given column
    * and return first nb rows in the sorted dataframe.
    *
    * @param sourceDF
    * @param columnName
    * @param nbResults number of top most occurrence values to return (by default 1).
    */
  def findMostOccurrenceValues(sourceDF: DataFrame,
                               columnName: String,
                               nbResults: Int*): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(sourceDF, columnName).sortByKey(false) // (descending)

    if (nbResults == 1 || mappedRDD.take(1).isEmpty)
      mappedRDD.take(1).map(_.swap)
    else
      {
        // There can be more tha one values that occurs same number of times.
        val nbTime = mappedRDD.first._1
        mappedRDD.filter(_._1 == nbTime)
          .map(_.swap)
          .collect()
          .sortWith(_._1.toString < _._1.toString)
      }
  }

  /**
    * Same as the previous method, but instead of sorting descending, this will sort ascending
    *
    * @param sourceDF
    * @param columnName
    * @param nbResults number of <i>least</i> occurrence values to return (by default 1).
    */
  def findLeastOccurrenceValues(sourceDF: DataFrame,
                                columnName: String,
                                nbResults: Int*): Iterable[(Any, Long)] = {

    val mappedRDD = mapNbOccurrencesToColValues(sourceDF, columnName).sortByKey(true) // (ascending)

    if (nbResults == 1 || mappedRDD.take(1).isEmpty)
      mappedRDD.take(1).map(_.swap)
    else
    {
      // There can be more tha one values that occurs same number of times.
      val nbTime = mappedRDD.first()._1
      mappedRDD.filter(_._1 == nbTime)
        .map(_.swap)
        .collect()
        .sortWith(_._1.toString < _._1.toString)
    }
  }
}