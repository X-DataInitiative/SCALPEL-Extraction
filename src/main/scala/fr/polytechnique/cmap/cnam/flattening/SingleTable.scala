package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig._


/**
  * Created by burq on 11/07/16.
  */
class SingleTable(config: Config, sqlContext: SQLContext) {
  import SingleTable._

  val name = config.name
  val pathList: List[String] = config.paths
  val tableName = config.tableName
  val key: String = config.key
  val dateFormat: String = if(config.hasPath("date_format")){
    config.getString("date_format")
  } else {
    ""
  }

  val schema: StructType = getSchema(config)

  lazy val df: DataFrame = if (dateFormat== "") {
    sqlContext.readCSV(schema, pathList.head)
  } else {
    sqlContext.readCSV(schema, pathList.head, dateFormat)
  }
}

object SingleTable {
  val Nullable: Boolean = true

  val Str2Type: Map[String, DataType] = Map( //todo matchcase because unittest
    "StringType"  -> StringType,
    "IntegerType" -> IntegerType,
    "LongType"    -> LongType,
    "DateType"    -> DateType,
    "DoubleType"  -> DoubleType
  )

  implicit class CSVContext(sqlContext: SQLContext) {
    def readCSV(
                 schema: StructType,
                 inputPath: String,
                 dateFormat: String = "dd/MM/yyyy"
               ): DataFrame = {
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("dateFormat", dateFormat)
        .schema(schema)
        .load(inputPath)
    }
  }

  def getSchema(tableConfig : Config): StructType = {
    StructType(
      tableConfig
        .getConfList("fields")
        .map(toStructField)
        .toArray
    )
  }

  def getSchema(fields : List[Config]): StructType = {
    StructType(
      fields
        .map(toStructField)
        .toArray
    )
  }

  def toStructField(config: Config): StructField = {
    StructField(config.name, Str2Type(config.dataType), Nullable)
  }

}
