package fr.polytechnique.cmap.cnam.filtering.ltsccs

import java.sql.Timestamp
import java.time.ZoneOffset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering._

// The following case classes are based on the formats defined in the following Confluence page:
//   https://datainitiative.atlassian.net/wiki/display/CNAM/Coxph+data+format
case class GroundTruth(
    drug_concept_id: String,
    drug_concept_name: String,
    condition_concept_id: String,
    condition_concept_name: String,
    ground_truth: Int)

case class Person(
    PatientID: String,
    EraType: String,
    EraSubType: Option[String],
    Start: String,
    End: Option[String])

case class ObservationPeriod(
    PatientID: String,
    EraType: String,
    EraSubType: Option[String],
    Start: String,
    End: String)

case class DrugExposure(
    PatientID: String,
    EraType: String,
    EraSubType: String,
    Start: String,
    End: String)

case class ConditionEra(
    PatientID: String,
    EraType: String,
    EraSubType: String,
    Start: String,
    End: String)

object LTSCCSWriter {

  private implicit class RichTimestamp(t: Timestamp) {
    def format: String = new java.text.SimpleDateFormat("yyyyMMdd").format(t)
    def minusMonths(m: Int): Timestamp = {
      Timestamp.from(t.toLocalDateTime.minusMonths(m).toInstant(ZoneOffset.ofHours(0)))
    }
  }

  private implicit class OutputData(data: DataFrame) {
    def writeCSV(path: String): Unit = {
      data
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .save(path)
    }
  }

  implicit class FlatEvents(data: Dataset[FlatEvent]) {

    import data.sqlContext.implicits._

    def groundTruth: Dataset[GroundTruth] = {
      Seq(
        GroundTruth("PIOGLITAZONE", "PIOGLITAZONE", "C67", "C67", 1),
        GroundTruth("INSULINE", "INSULINE", "C67", "C67", 0),
        GroundTruth("SULFONYLUREA", "SULFONYLUREA", "C67", "C67", 0),
        GroundTruth("METFORMINE", "METFORMINE", "C67", "C67", 0),
        GroundTruth("BENFLUOREX", "BENFLUOREX", "C67", "C67", 0),
        GroundTruth("ROSIGLITAZONE", "ROSIGLITAZONE", "C67", "C67", 0),
        GroundTruth("OTHER", "OTHER", "C67", "C67", 0)
      ).toDS
    }

    def filterPatients(patientIDs: Dataset[String]): Dataset[FlatEvent] = {
      data.as("e").joinWith(patientIDs.as("p"), col("e.patientID") === col("p.value")).map {
        case (event: FlatEvent, patientID: String) => event
      }
    }

    def toPersons: Dataset[Person] = {
      data.map(e => Person(e.patientID, "Patient", None, e.birthDate.format, None)).distinct
    }

    def toObservationPeriods: Dataset[ObservationPeriod] = {
      data.map(
        obs => ObservationPeriod(
          obs.patientID,
          "ObservationPeriod",
          None,
          obs.start.minusMonths(6).format,
          obs.end.get.format
        )
      ).distinct
    }

    def toDrugExposures: Dataset[DrugExposure] = {
      data.map(
        e => DrugExposure(e.patientID, "Rx", e.eventId, e.start.format, e.end.get.format)
      ).distinct
    }

    def toConditionEras: Dataset[ConditionEra] = {
      data.map(
        disease => ConditionEra(
          disease.patientID,
          "Condition",
          disease.eventId,
          disease.start.format,
          disease.start.format
        )
      ).distinct
    }

    def writeLTSCCS(path: String): Unit = {
      val dir = if(path.last == '/') path else path + "/"

      val exposures: Dataset[FlatEvent] = data.filter(_.category == "exposure").persist()
      val patients = exposures.toDF.dropDuplicates(Seq("patientID")).as[FlatEvent].persist()
      val observationPeriods: Dataset[FlatEvent] = data.filter(_.category == "observationPeriod").persist()
      val diseases: Dataset[FlatEvent] = data.filter(_.category == "disease").persist()

      val patientIDs: Dataset[String] = patients.map(_.patientID).persist()

      groundTruth.toDF.coalesce(1)
        .writeCSV(dir + "GroundTruth.csv")
      patients.toPersons.toDF.coalesce(1)
        .writeCSV(dir + "Persons.txt")
      observationPeriods.filterPatients(patientIDs).toObservationPeriods.toDF.coalesce(1)
        .writeCSV(dir + "ObservationPeriods.txt")
      exposures.toDrugExposures.toDF.coalesce(1)
        .writeCSV(dir + "Drugexposures.txt")
      diseases.filterPatients(patientIDs).toConditionEras.toDF.coalesce(1)
        .writeCSV(dir + "Conditioneras.txt")

      patientIDs.unpersist()
      diseases.unpersist()
      observationPeriods.unpersist()
      exposures.unpersist()
    }
  }
}
