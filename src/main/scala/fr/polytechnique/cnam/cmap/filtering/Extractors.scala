package fr.polytechnique.cnam.cmap.filtering

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Base trait for all Extractor classes
  *
  * @author Daniel de Paula
  */
trait Extractor {
  def extract(path: String): DataFrame
}

/**
 * Extractor class for the DCIR table
 *
 * @author Daniel de Paula
 */
class DcirExtractor(sqlContext: SQLContext) extends Extractor {

  def extract(path: String): DataFrame = {
    // todo: implement extraction
    sqlContext.emptyDataFrame
  }
}

/**
 * Extractor class for the P_MCO table
 *
 * @author Daniel de Paula
 */
class PmsiMcoExtractor(sqlContext: SQLContext) extends Extractor {

  def extract(path: String): DataFrame = {
    // todo: implement extraction
    sqlContext.emptyDataFrame
  }
}

/**
  * Extractor class for the P_HAD table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class PmsiHadExtractor(sqlContext: SQLContext) extends Extractor {

  def extract(path: String): DataFrame = {
    // todo: implement extraction
    sqlContext.emptyDataFrame
  }
}

/**
  * Extractor class for the P_SSR table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class PmsiSsrExtractor(sqlContext: SQLContext) extends Extractor {

  def extract(path: String): DataFrame = {
    // todo: implement extraction
    sqlContext.emptyDataFrame
  }
}

/**
  * Extractor class for the IR_BEN_R table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class IrBenExtractor(sqlContext: SQLContext) extends Extractor {

  def extract(path: String): DataFrame = {
    // todo: implement extraction
    sqlContext.emptyDataFrame
  }
}

/**
  * Extractor class for the IR_IMB_R table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class IrImbExtractor(sqlContext: SQLContext) extends Extractor {

  def extract(path: String): DataFrame = {
    // todo: implement extraction
    sqlContext.emptyDataFrame
  }
}

/**
  * Extractor class for the IR_PHA_R table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class IrPhaExtractor(sqlContext: SQLContext) extends Extractor {

  def extract(path: String): DataFrame = {
    // todo: implement extraction
    sqlContext.emptyDataFrame
  }
}



