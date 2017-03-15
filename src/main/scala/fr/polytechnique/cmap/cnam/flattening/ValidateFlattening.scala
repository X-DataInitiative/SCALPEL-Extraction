package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.statistics.Comparator
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._

/**
  * Created by sathiya on 21/09/16.
  */
object ValidateFlattening extends Main {

  override def appName: String = "ValidateFlattening"

  implicit class ConvertColNameDelimiters(data: DataFrame) {

    final val OldDelimiter: String = "\\."
    final val NewDelimiter: String = "__"

    def prefixColumnNameWithDelimiter(prefix: String, ignoreColumns: Seq[String]): Seq[Column] = {
      data.columns
        .map(
          columnName => {
            if (ignoreColumns.contains(columnName))
              col(columnName)
            else
              col(columnName).as(prefix + NewDelimiter + columnName)
          })
    }

    def changeColumnNameDelimiter: Seq[Column] = {
      data.columns
        .map(
          columnName => {
            val splittedColName = columnName.split(OldDelimiter)
            if (splittedColName.size == 2)
              col("`" + columnName + "`").as(splittedColName(0) + NewDelimiter + splittedColName(1))
            else
              col(columnName)
          })
    }

    def prefixColumnNames(prefix: String): DataFrame = {
      data.select(prefixColumnNameWithDelimiter(prefix, Nil): _*)
    }

    def prefixColumnNames(prefix: String, ignoreColumns: Seq[String]): DataFrame = {
      data.select(prefixColumnNameWithDelimiter(prefix, ignoreColumns): _*)
    }

    def cleanDFColumnNames: DataFrame = {
      data.select(changeColumnNameDelimiter: _*)
    }

    def computeStatForComparison: DataFrame = {
      Comparator.unifyColNamesAndDescribeDistinct(data)
    }

    def computeStatForComparison(subDfs: Seq[DataFrame]): DataFrame = {
      Comparator.unifyColNamesAndDescribeDistinct(data, subDfs: _*)
    }
  }

  implicit class JoinedTableUtilities(joinedTableObject: JoinedTable) {

    def getFlatDfWithCleanColumnNames: DataFrame = {
      joinedTableObject
        .joinedTable
        .cleanDFColumnNames
    }

    def getSubDfWithPrefixedColumnNames: Seq[DataFrame] = {
      joinedTableObject
        .otherTables
        .map {
          case (subDfName: String, df: DataFrame) =>
            df.prefixColumnNames(subDfName, joinedTableObject.joinKeys)
        }.toSeq
    }
  }

  def getJoinedTablePairsFromConfig: Map[String, JoinedTable] = {

    FlatteningConfig.joinTablesConfig
      .map(
        config => {
          config.name -> new JoinedTable(config, sqlContext)
        }
      ).toMap
  }

  def storeFlatAndInputDfsStat(flatDfName: String,
                               flatDfStat: DataFrame,
                               inputDfsStat: DataFrame) = {
    flatDfStat.writeParquet(FlatteningConfig.outputPath + s"/joins/stat/${flatDfName}_flat")
    inputDfsStat.writeParquet(FlatteningConfig.outputPath + s"/joins/stat/${flatDfName}_input")
  }

  def computeStoreFlatAndInputDfsStat() = {
    val joinedTablePairs = getJoinedTablePairsFromConfig

    joinedTablePairs.foreach {
      case (flatDfName, joinedTableObject) => {
        val cleanedFlatDf: DataFrame = joinedTableObject.getFlatDfWithCleanColumnNames
        val cleanedSubDfs: Seq[DataFrame] = joinedTableObject.getSubDfWithPrefixedColumnNames
        val mainTable: DataFrame = joinedTableObject.mainTable

        val flatDfStat = cleanedFlatDf.computeStatForComparison
        val inputDfsStat = mainTable.computeStatForComparison(cleanedSubDfs)

        storeFlatAndInputDfsStat(flatDfName, flatDfStat, inputDfsStat)
      }
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    computeStoreFlatAndInputDfsStat()
    None
  }
}
