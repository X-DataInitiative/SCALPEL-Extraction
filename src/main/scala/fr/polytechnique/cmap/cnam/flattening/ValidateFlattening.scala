package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.statistics.Comparator
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig._

/**
  * Created by sathiya on 21/09/16.
  */
object ValidateFlattening extends Main {

  override def appName: String = "ValidateFlattening"

  implicit class ConvertColNameDelimiters(data: DataFrame) {

    val oldDelimiter: String = "\\."
    val newDelimiter: String = "_"

    def prefixColumnNameWithDelimiter(prefix: String, ignoreColumns: Seq[String]): Seq[Column] = {
      data.columns
        .map(
          columnName => {
            if (ignoreColumns.contains(columnName))
              col(columnName)
            else
              col(columnName).as(prefix + newDelimiter + columnName)
          })
    }

    def changeColumnNameDelimiter: Seq[Column] = {
      data.columns
        .map(
          columnName => {
            val splittedColName = columnName.split(oldDelimiter)
            if (splittedColName.size == 2)
              col("`" + columnName + "`").as(splittedColName(0) + newDelimiter + splittedColName(1))
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
  }

  def validateJoinedTables() = {
    val joinTables = FlatteningConfig.joinTablesConfig
      .map(
        config => {
          config.name -> new JoinedTable(config, sqlContext)
        }
      ).toMap

    joinTables.foreach {
      case (dfName, joinedTable) => {
        val cleanedFlatDf = joinedTable.joinedTable.cleanDFColumnNames

        val cleanedSubDfs: Seq[DataFrame] = joinedTable.otherTables.
          map(nameDfPair => nameDfPair._2.prefixColumnNames(nameDfPair._1,
            joinedTable.joinKeys)).toSeq

        val flatDfResult = Comparator.unifyColNamesAndDescribeDistinct(cleanedFlatDf)
        val inputDfsResult = Comparator.unifyColNamesAndDescribeDistinct(joinedTable.mainTable,
          cleanedSubDfs: _*)

        flatDfResult.writeParquet(FlatteningConfig.outputPath + s"/joins/stat/${dfName}_flat")
        inputDfsResult.writeParquet(FlatteningConfig.outputPath + s"/joins/stat/${dfName}_input")

        flatDfResult.select("ColName").collect foreach println
        println()
        inputDfsResult.select("ColName").collect foreach println
      }
    }
  }

  def main(args: Array[String]){
    startContext( )
    validateJoinedTables()
  }
}
