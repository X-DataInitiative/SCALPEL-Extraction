package fr.polytechnique.cmap.cnam.flattening

import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._

/**
  * Created by burq on 19/07/16.
  */
object Converter {

  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf().setAppName("Flattening")

  Locale.setDefault(Locale.US)

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "104857600")

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


  def main(args: Array[String]): Unit = {
    import sqlContext.implicits._

    loadToParquet()

    joinTables()

  }
}
