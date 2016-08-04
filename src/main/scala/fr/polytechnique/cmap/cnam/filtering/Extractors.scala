package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Base class for all Extractor classes.
  * Note that all Extractors need an sqlContext, so this attribute is defined in the base class.
  *
  * @author Daniel de Paula
  */
abstract class Extractor(sqlContext: SQLContext) {

/**
  * The base IO operation for reading a table.
  * All subclasses that override this method should call super.extract as the first operation.
  */
  def extract(path: String): DataFrame = {
    sqlContext.read.parquet(path)
  }
}

/**
  * Extractor class for the DCIR table
  *
  * @author Daniel de Paula
  */
class DcirExtractor(sqlContext: SQLContext) extends Extractor(sqlContext) {

/**
  * Reads and filters data from a DCIR file
  */
  override def extract(dcirPath: String): DataFrame = {
    super.extract(dcirPath)
      .where(col("BSE_PRS_NAT") !== 0)
  }
}

/**
  * Extractor class for the P_MCO table
  *
  * @author Daniel de Paula
  */
class PmsiMcoExtractor(sqlContext: SQLContext) extends Extractor(sqlContext) {

  override def extract(path: String): DataFrame = {
    val rawDF = super.extract(path)
    // todo: implement extraction
    rawDF
  }
}

/**
  * Extractor class for the P_HAD table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class PmsiHadExtractor(sqlContext: SQLContext) extends Extractor(sqlContext) {

  override def extract(path: String): DataFrame = {
    val rawDF = super.extract(path)
    // todo: implement extraction
    rawDF
  }
}

/**
  * Extractor class for the P_SSR table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class PmsiSsrExtractor(sqlContext: SQLContext) extends Extractor(sqlContext) {

  override def extract(path: String): DataFrame = {
    val rawDF = super.extract(path)
    // todo: implement extraction
    rawDF
  }
}

/**
  * Extractor class for the IR_BEN_R table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class IrBenExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)

/**
  * Extractor class for the IR_IMB_R table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class IrImbExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)
/**
  * Extractor class for the IR_PHA_R table
  *
  * @author Daniel de Paula
  * @param sqlContext
  */
class IrPhaExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)