package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.extractors.EventRowExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.Columns

class DrugEventExtractor(config: DrugConfig) extends java.io.Serializable {

  object ColNames extends Columns {
    final lazy val PatientID: String = "NUM_ENQ"
    final lazy val CIP13: String = "ER_PHA_F__PHA_PRS_C13"
    final lazy val ATC5: String = "PHA_ATC_C07"
    final lazy val EventDate: String = "EXE_SOI_DTD"
    final lazy val Molecules: String = "molecule_combination"
    final lazy val Conditioning: String = "PHA_CND_TOP"
  }

  final lazy val PHA_CIP_C13: String = "PHA_CIP_C13"
  final lazy val COND_GC: String = "GC"

  def extract(sources : Sources): Dataset[Event[Drug]] = {
    val irPhaR = sources.irPha.get
    val dcir = sources.dcir.get
    val spark: SparkSession = dcir.sparkSession
    val df: DataFrame = dcir.join(
      irPhaR, dcir.col(ColNames.CIP13) === irPhaR.col(PHA_CIP_C13))
    import spark.implicits._
    df
      .select(ColNames.getColumns.map(functions.col): _*)
      .withColumn(ColNames.Conditioning, when(col(ColNames.Conditioning) === COND_GC, 1).otherwise(2))
      .na.drop(Seq(ColNames.EventDate, ColNames.CIP13, ColNames.ATC5))
      .flatMap{ r : Row =>
        config.level(new PurchaseDAO(
          r.getAs[String](ColNames.PatientID),
          r.getAs[String](ColNames.CIP13),
          r.getAs[String](ColNames.ATC5),
          r.getAs[Timestamp](ColNames.EventDate),
          r.getAs[String](ColNames.Molecules),
          r.getAs[Int](ColNames.Conditioning)), config.families)
      }.distinct
  }
}
