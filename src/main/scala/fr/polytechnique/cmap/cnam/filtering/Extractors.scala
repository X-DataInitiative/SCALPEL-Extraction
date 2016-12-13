package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Base class for all Extractor classes.
  * Note that all Extractors need an sqlContext, so this attribute is defined in the base class.
  *
  */
abstract class Extractor(sqlContext: SQLContext) {

  def extract(path: String): DataFrame = sqlContext.read.parquet(path)

}

/**
  * Extractor class for the DCIR table
  *
  */
class DcirExtractor(sqlContext: SQLContext) extends Extractor(sqlContext) {

/**
  * Reads and filters data from a DCIR file
  * The column "BSE_PRS_NAT" should never be 0
  */
  override def extract(dcirPath: String): DataFrame = {
    super.extract(dcirPath)
      .where(col("BSE_PRS_NAT") !== 0)
  }
}

/**
  * Extractor class for the MCO table
  * This filtering is explained here
  * https://datainitiative.atlassian.net/wiki/pages/viewpage.action?pageId=40304642
  *
  */
class McoExtractor(sqlContext: SQLContext) extends Extractor(sqlContext) {
  val specialHospitals = List("130780521", "130783236", "130783293", "130784234", "130804297",
    "600100101", "690783154", "690784137", "690784152", "690784178", "690787478", "750041543",
    "750100018", "750100042", "750100075", "750100083", "750100091", "750100109", "750100125",
    "750100166", "750100208", "750100216", "750100232", "750100273", "750100299", "750801441",
    "750803447", "750803454", "830100558", "910100015", "910100023", "920100013", "920100021",
    "920100039", "920100047", "920100054", "920100062", "930100011", "930100037", "930100045",
    "940100027", "940100035", "940100043", "940100050", "940100068", "950100016")

  override def extract(path: String): DataFrame = {
    super.extract(path).filter(!col("ETA_NUM").isin(specialHospitals:_*))
  }
}


/**
  * Extractor class for the P_HAD table
  *
  */
class HadExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)

/**
  * Extractor class for the P_SSR table
  *
  */
class SsrExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)

/**
  * Extractor class for the IR_BEN_R table
  *
  */
class IrBenExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)

/**
  * Extractor class for the IR_IMB_R table
  *
  */
class IrImbExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)
{
  override def extract(irImbPath: String): DataFrame = {
    super.extract(irImbPath)
      //.where(col("IMB_ALD_DTD") isNotNull)
      .where(col("IMB_ALD_DTD") !== (""))
  }

}
/**
  * Extractor class for the IR_PHA_R table
  *
  */
class IrPhaExtractor(sqlContext: SQLContext) extends Extractor(sqlContext)

/**
  * Extractor for the hand-generated CSV that list all the dosage
  */
class DrugDosageExtractor(sqlContext: SQLContext) extends Extractor(sqlContext) {

  override def extract(path: String): DataFrame = {
    sqlContext.read
      .format("csv")
      .option("header", "true")
      .load(path)
      .select(
        col("PHA_PRS_IDE"),
        col("MOLECULE_NAME"),
        col("TOTAL_MG_PER_UNIT").cast(IntegerType)
      )
      .where(col("MOLECULE_NAME") !== "BENFLUOREX")
  }

}