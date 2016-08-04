package fr.polytechnique.cmap.cnam.statistics

import fr.polytechnique.cmap.cnam.SharedContext
import org.apache.spark.sql.SQLContext

/**
  * Created by sathiya on 04/08/16.
  *
  * This Class contains all shared variables for statistics TestSuits
  */
trait Config extends SharedContext {

  val _sqlContext: SQLContext = sqlContext
  val srcFilePath: String = "src/test/resources/IR_BEN_R.csv"
  // The older versions of the csv package don't convert empty values to null.
  // So the functions should give consistent output whether or not
  // the csv package converts empty values to null.
  val srcDF = sqlContext.read.format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .option("treatEmptyValuesAsNulls", "true")
    .option("nullValue", "")
    .load(srcFilePath)
}