package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._

object FlatteningMain extends Main {

  def appName = "Flattening"

  def loadToParquet(): Unit = {
    val tables = FlatteningConfig.tablesConfig
      .map(
        config =>
          config.name -> new SingleTable(config, sqlContext)
      ).toMap

    tables.foreach{
      case(_, value) =>
        println("################################################################################")
        println("SINGLE WRITE " + value.tableName +
          " at year " + value. key +
          " format " + value.dateFormat)
        println("################################################################################")
        value.df.writeParquet(
          FlatteningConfig.outputPath + "/" + value.tableName + "/key=" + value.key
        )
        value.df.unpersist()
    }
  }

  def joinTables(): Unit = {
    val joinTables = FlatteningConfig.joinTablesConfig
      .map(
        config => {
          config.name -> new JoinedTable(config, sqlContext)
        }
      ).toMap

    joinTables.foreach{
      case(_, value) => {
        println("################################################################################")
        println("JOINING " + value.name )
        value.otherTables.keys.foreach(key => println("- " + key))
        value.joinedDFPerYear.foreach {
          case (year: Int, df: DataFrame) => {
            println("################################################################################")
            println("WRITING " + value.name + " at year " + year)
            df.writeParquet(
              FlatteningConfig.outputPath + "/joins/" + value.name + "/key=" + year
            )
            df.unpersist()
            println("DONE " + value.name + " at year " + year)
            println("################################################################################")
          }
        }
      }
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    loadToParquet()
    joinTables()
    None
  }
}