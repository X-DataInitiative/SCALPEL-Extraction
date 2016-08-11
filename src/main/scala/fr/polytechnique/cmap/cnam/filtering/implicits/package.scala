package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._

package object implicits {

  /**
    * Implicit class for writing the events to a parquet file
    *
    * todo: We should decide if this class should stay here or in an object "Writer", importable by doing "import Writer._"
    */
  implicit class DataWriter(data: Dataset[Event]) {

    def writeFlatEvent(patient: Dataset[Patient], path: String): Unit = {
      import data.sqlContext.implicits._

      val flatEventColumns: List[Column] = List(
        col("patientID"),
        col("gender"),
        col("birthDate"),
        col("deathDate"),
        col("category"),
        col("eventId"),
        col("weight"),
        col("start"),
        col("end")
      )

      data.as("left")
        .joinWith(patient.as("right"), $"left.patientID" === $"right.patientID")
        .map((FlatEvent.convert _).tupled)
        .toDF()
        .write
        .parquet(path)
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
