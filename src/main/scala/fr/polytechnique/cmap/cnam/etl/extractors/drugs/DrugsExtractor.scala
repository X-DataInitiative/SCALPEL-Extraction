package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugClassificationLevel._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql._

case class Purchase(patientID: String, CIP13: String, ATC5: String = "", eventDate: Timestamp)


object DrugsExtractor extends java.io.Serializable{

  def formatSource(sources : Sources): Dataset[Purchase] = {

    val neededColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("ER_PHA_F__PHA_PRS_C13").cast(StringType).as("CIP13"),
      col("PHA_ATC_C07").cast(StringType).as("ATC5"),
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate")
    )

    lazy val irPhaR = sources.irPha.get
    lazy val dcir = sources.dcir.get
    val spark: SparkSession = dcir.sparkSession
    import spark.implicits._

    lazy val df: DataFrame = dcir.join(irPhaR, dcir.col("ER_PHA_F__PHA_PRS_C13") === irPhaR.col("PHA_CIP_C13"))

    df
      .select(neededColumns: _*)
      .na.drop(Seq("eventDate", "CIP13", "ATC5"))
      .as[Purchase]
  }

  def buildDrugEvent(patientID: String, drugFamily: List[String], eventDate: Timestamp): List[Event[Drug]] = {
    drugFamily.map(family => Drug(patientID, family, 0, eventDate))
  }

  def getCorrectDrugCodes (level: DrugClassificationLevel, drugPurchases : Dataset[Purchase], drugFamilies: List[DrugConfig]): Dataset[Event[Drug]] = {

    val spark: SparkSession = drugPurchases.sparkSession
    import spark.implicits._

    drugPurchases
      .flatMap { row: Purchase =>
        val families = level match {
          case DrugClassificationLevel.Therapeutic =>
            drugFamilies.filter(family => family.cip13Codes.contains(row.CIP13))
              .map(_.name)
          case DrugClassificationLevel.Pharmacological =>
            drugFamilies.flatMap(_.pharmacologicalClasses)
              .filter(family => family.isCorrect(row.ATC5, ""))
              .map(_.name)
        }

        buildDrugEvent(row.patientID, families, row.eventDate)
      }
  }

  def extract (level: DrugClassificationLevel, sources : Sources, drugFamilies: List[DrugConfig]): Dataset[Event[Drug]] = {

    val drugPurchases = formatSource(sources)
    getCorrectDrugCodes(level, drugPurchases, drugFamilies)
  }

}
