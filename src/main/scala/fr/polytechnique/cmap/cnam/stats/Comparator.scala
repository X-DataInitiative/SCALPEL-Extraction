package fr.polytechnique.cmap.cnam.stats

import fr.polytechnique.cmap.cnam.stats.CustomStatistics._
import fr.polytechnique.cmap.cnam.util.RichDataFrames._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by sathiya on 12/08/16.
  */
object Comparator {
  //Todo: This method works only when there is no unseen keys (key columns) in the individual DFs
  // that don't exists in the DF that contains the key (eq, PRS for DCIR, C for PMSI MCO ).
  // We should waive this restriction in the future versions.

  def unifyColNamesAndDescribeDistinct(singleDF: DataFrame, otherDFs: DataFrame*): DataFrame = {

    val tables: Seq[DataFrame] = Seq(singleDF) ++ otherDFs.toList

    val columnsToCheck: Set[String] = {
    if(otherDFs.isEmpty)
      singleDF.columns.toSet
    else
      singleDF.columns.toSet ++ otherDFs.map(_.columns).reduce(_ ++ _).toSet
    }

    columnsToCheck
      .map(
        colName => {
          tables
            .collect { case table if table.columns.contains(colName) => table.select(col(colName)) }
            .reduce(_.unionAll(_)).customDescribe(forComparison = true)
        }
      ).reduce(_.unionAll(_))
  }

  def compare(flatDF: DataFrame, singleDF: DataFrame, otherDFs: DataFrame*): Boolean = {

    val flatDFResult: DataFrame = flatDF.customDescribe(forComparison = true)

    val chunckDfsResult: DataFrame = Comparator.unifyColNamesAndDescribeDistinct(singleDF, otherDFs:_*)

    (flatDFResult === chunckDfsResult)
  }
}