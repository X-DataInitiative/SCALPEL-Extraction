package fr.polytechnique.cmap.cnam.filtering

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

package object implicits {

  /**
    * Implicit class for writing the events to a parquet file
    *
    * todo: We should decide if this class should stay here or in an object "Writer", importable by doing "import Writer._"
    */
  implicit class DataWriter[T](data: Dataset[T]) {
    def writeData(path: String): Unit = {

    }
  }

  /**
    * Implicit class for extracting data using an sqlContext directly (not as an argument)
    *
    */
  implicit class SourceExtractor(sqlContext: SQLContext) {

    def extractDcir(path: String): DataFrame = new DcirExtractor(this.sqlContext).extract(path)
    def extractPmsiMco(path: String): DataFrame = new McoExtractor(this.sqlContext).extract(path)
    def extractPmsiHad(path: String): DataFrame = new HadExtractor(this.sqlContext).extract(path)
    def extractPmsiSsr(path: String): DataFrame = new SsrExtractor(this.sqlContext).extract(path)
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
