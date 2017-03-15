package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 04/08/16.
  *
  * This Class contains all shared variables for statistics TestSuits
  */
trait Config extends SharedContext {

  def getSourceDF: DataFrame = {
    val srcFilePath: String = "src/test/resources/statistics/IR_BEN_R.csv"
    // The older versions of the csv package don't convert empty values to null.
    // So the functions should give consistent output whether or not
    // the csv package converts empty values to null.
    val sourceDF = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .option("treatEmptyValuesAsNulls", "true")
      .load(srcFilePath)
    sourceDF
  }
}