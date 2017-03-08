package fr.polytechnique.cmap.cnam.flattening


import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.util.FlatteningConfig
import fr.polytechnique.cmap.cnam.util.FlatteningConfig._
import JoinedTable._

/**
  * Created by burq on 13/07/16.
  */
class JoinedTable(config: Config, sqlContext: SQLContext){
  import sqlContext.implicits._

  val name: String = config.name
  val joinKeys: List[String] = config.columns

  lazy val mainTable: DataFrame = sqlContext
    .read
    .option("mergeSchema", "true")
    .parquet(path + "/" + config.mainTable)

  lazy val joinedTable: DataFrame = sqlContext
    .read
    .option("mergeSchema", "true")
    .parquet(path + "/joins/" + config.name)

  var isPartitioned: Boolean = false

  lazy val years: Array[Int] = {
    if(mainTable.select($"key").distinct.cache.count==1) {
      isPartitioned = false
      mainTable.select(year($"FLX_TRT_DTD")).distinct.collect().map(_.getInt(0))
    } else {
      isPartitioned = true
      mainTable.select($"key").distinct.collect().map(_.getInt(0))
    }
  }

  lazy val otherTables: Map[String, DataFrame] = config.tables.map(
    name =>
      name ->
        sqlContext
        .read
        .option("mergeSchema", "true")
        .parquet(path + "/" + name)
  ).toMap

  lazy val otherTablesWithPrefix: Traversable[DataFrame] = otherTables.map((addPrefix _).tupled)

  lazy val filterColumn: Column = if(isPartitioned){
    $"key"
  }else{
    year($"FLX_TRT_DTD")
  }

  lazy val joinedDFPerYear: Map[Int,DataFrame] = years.map(joinDataFrames).toMap

  def joinDataFrames(year: Int): (Int, DataFrame) = {
    year ->
      otherTablesWithPrefix.map{
        _.filter(filterColumn === year).drop($"key")
      }.foldLeft(mainTable.filter(filterColumn === year))(joinFunction)
  }

  def joinFunction(accumulator: DataFrame, other: DataFrame): DataFrame = {
    val _joinKeys = joinKeys
    accumulator.join(other, _joinKeys, "left_outer")
  }

  def addPrefix(prefix: String, df: DataFrame): DataFrame = {
    val _joinKeys = "key" :: joinKeys
    df.select(
      df.columns.map{
        columnName =>
          if(_joinKeys.contains(columnName)){
            col(columnName)
          }else{
            col(columnName).as(prefix + "." + columnName)
          }
      }: _*
    )
  }

}

object JoinedTable {

  val path: String = FlatteningConfig.outputPath

}