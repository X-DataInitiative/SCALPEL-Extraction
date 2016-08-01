package fr.polytechnique.cnam.cmap.filtering

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

/**
  * @author Daniel de Paula
  */
package object implicits {

  /**
    * Implicit class for writing the events to a parquet file
    *
    * @author Daniel de Paula
    */
  implicit class DataWriter[T](data: Dataset[T]) {
    def writeData(path: String): Unit = {

    }
  }

  /**
    * Implicit class for extracting data using an sqlContext directly
    *
    * @author Daniel de Paula
    */
  implicit class SourceExtractor(sqlContext: SQLContext) {

    def extractDcir(path: String): DataFrame = new DcirExtractor(this.sqlContext).extract(path)
    def extractPmsiMco(path: String): DataFrame = new PmsiMcoExtractor(this.sqlContext).extract(path)
    def extractPmsiHad(path: String): DataFrame = new PmsiHadExtractor(this.sqlContext).extract(path)
    def extractPmsiSsr(path: String): DataFrame = new PmsiSsrExtractor(this.sqlContext).extract(path)
    def extractIrBen(path: String): DataFrame = new IrBenExtractor(this.sqlContext).extract(path)
    def extractIrImb(path: String): DataFrame = new IrImbExtractor(this.sqlContext).extract(path)
    def extractIrPha(path: String): DataFrame = new IrPhaExtractor(this.sqlContext).extract(path)

    def extractAll(pathConfig: Config): Sources = {
      new Sources(
        dcir = Some(extractDcir(pathConfig.getString("dcir"))),
        pmsiMco = Some(extractPmsiMco(pathConfig.getString("pmsiMco"))),
        pmsiHad = Some(extractPmsiHad(pathConfig.getString("pmsiHad"))),
        pmsiSsr = Some(extractPmsiSsr(pathConfig.getString("pmsiSsr"))),
        irBen = Some(extractIrBen(pathConfig.getString("irBen"))),
        irImb = Some(extractIrImb(pathConfig.getString("irImb"))),
        irPha = Some(extractIrPha(pathConfig.getString("irPha")))
      )
    }
  }
}
