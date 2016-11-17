package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.filtering.FilteringConfig.InputPaths


package object implicits {

  /**
    * Implicit class for writing the events to a parquet file
    *
    * TODO: We should decide if this class should stay here or in an object "Writer", importable by
    * doing "import Writer._"
    */
  implicit class FlatEventWriter(data: Dataset[Event]) {

    def writeFlatEvent(patient: Dataset[Patient], path: String): Unit = {
      import data.sqlContext.implicits._

      data.as("left")
        .joinWith(patient.as("right"), $"left.patientID" === $"right.patientID")
        .map((FlatEvent.merge _).tupled)
        .toDF()
        .write
        .parquet(path)
    }
  }

  /**
    * Implicit class for extracting data using an sqlContext directly (not as an argument)
    */
  implicit class SourceExtractor(sqlContext: SQLContext) {

    def extractDcir(path: String): DataFrame = new DcirExtractor(this.sqlContext).extract(path)
    def extractPmsiMco(path: String): DataFrame = new McoExtractor(this.sqlContext).extract(path)
    def extractPmsiHad(path: String): DataFrame = new HadExtractor(this.sqlContext).extract(path)
    def extractPmsiSsr(path: String): DataFrame = new SsrExtractor(this.sqlContext).extract(path)
    def extractIrBen(path: String): DataFrame = new IrBenExtractor(this.sqlContext).extract(path)
    def extractIrImb(path: String): DataFrame = new IrImbExtractor(this.sqlContext).extract(path)
    def extractIrPha(path: String): DataFrame = new IrPhaExtractor(this.sqlContext).extract(path)
    def extractDosages(path: String): DataFrame = new DrugDosageExtractor(this.sqlContext).extract(path)

    def extractAll(paths: InputPaths): Sources = {
      new Sources(
        dcir = Some(extractDcir(paths.dcir)),
        pmsiMco = Some(extractPmsiMco(paths.pmsiMco)),
        // pmsiHad = Some(extractPmsiHad(paths.pmsiHad)),
        // pmsiSsr = Some(extractPmsiSsr(paths.pmsiSsr)),
        irBen = Some(extractIrBen(paths.irBen)),
        irImb = Some(extractIrImb(paths.irImb)),
        irPha = Some(extractIrPha(paths.irPha)),
        dosages = Some(extractDosages(paths.dosages))
      )
    }

    /**
      * For backwards compatibility only
      * @deprecated
      */
    def extractAll(pathConfig: Config): Sources = {
      new Sources(
        dcir = Some(extractDcir(pathConfig.getString("dcir"))),
        pmsiMco = Some(extractPmsiMco(pathConfig.getString("pmsi_mco"))),
        // pmsiHad = Some(extractPmsiHad(pathConfig.getString("pmsi_had"))),
        // pmsiSsr = Some(extractPmsiSsr(pathConfig.getString("pmsi_ssr"))),
        irBen = Some(extractIrBen(pathConfig.getString("ir_ben"))),
        irImb = Some(extractIrImb(pathConfig.getString("ir_imb"))),
        irPha = Some(extractIrPha(pathConfig.getString("ir_pha"))),
        dosages = Some(extractDosages(pathConfig.getString("dosages")))
      )
    }
  }
}
