package fr.polytechnique.cmap.cnam.filtering.ltsccs

import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering._
import fr.polytechnique.cmap.cnam.utilities.functions._

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

  final val StudyStart = makeTS(2006, 1, 1)
  final val StudyEnd = makeTS(2009, 12, 31, 23, 59, 59)
  final val DiseaseCodes = List("targetDisease")
  final val Molecules = List("ALL")

  private implicit class RichTimestamp(t: Timestamp) {
    def format: String = new java.text.SimpleDateFormat("yyyyMMdd").format(t)
  }

  private implicit class OutputData(data: DataFrame) {
    def writeCSV(path: String): Unit = {
      data
        .write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("nullValue", "")
        .save(path)
    }
  }

  implicit class FlatEvents(data: Dataset[FlatEvent]) {

    import data.sqlContext.implicits._

    def groundTruth(moleculeNames: List[String], diseaseCodes: List[String]): Dataset[GroundTruth] = {
      val list = for(m <- moleculeNames; d <- diseaseCodes) yield GroundTruth(m, m, d, d, 1)
      list.toDS
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
          "ObsPeriod",
          None,
          obs.start.format,
          obs.end.get.format
        )
      ).distinct
    }

    def toDrugExposures: Dataset[DrugExposure] = {
      data
        .filter(e => e.start.after(StudyStart) && e.start.before(StudyEnd))
        .map(
          e => DrugExposure(e.patientID, "Rx", e.eventId, e.start.format, e.end.get.format)
        ).distinct
    }

    def toConditionEras: Dataset[ConditionEra] = {
      val result = data
        .filter(e => e.start.after(StudyStart) && e.start.before(StudyEnd))
        .map(
          disease => ConditionEra(
            disease.patientID,
            "Condition",
            disease.eventId,
            disease.start.format,
            disease.start.format
          )
        ).distinct
      result
    }

    def writeLTSCCS(path: String): Unit = {
      val rootDir = if (path.last == '/') path.dropRight(1) else path

      val observationPeriods: Dataset[FlatEvent] = data.filter(_.category == "observationPeriod")
        .persist()
      val diseases: Dataset[FlatEvent] = data.filter(e => e.category == "disease" && e.eventId == "targetDisease")
        .persist()
      val exposures: Dataset[FlatEvent] = data.filter(_.category == "exposure")
        .persist()
      val allMolecules: List[String] = exposures.map(_.eventId).distinct.collect.toList

      for (population <- List("all", "men"); molecule <- Molecules) {

        val moleculeExposures = {
          if(molecule == "ALL") exposures
          else exposures.filter(_.eventId == molecule)
        }
        val moleculesList = {
          if(molecule == "ALL") allMolecules
          else List(molecule)
        }

        val rawPatients = moleculeExposures.toDF.dropDuplicates(Seq("patientID")).as[FlatEvent]
          .persist()

        val patients = {
          if (population == "men") rawPatients.filter(_.gender == 1)
          else rawPatients
        }
        val patientIDs = patients.map(_.patientID)

        val dir = s"$rootDir/$population/$molecule"
        Logger.getLogger(getClass).info(s"Writing $dir...")

        groundTruth(moleculesList, DiseaseCodes).toDF.coalesce(1)
          .writeCSV(s"$dir/GroundTruth.csv")
        patients.toPersons.toDF.coalesce(1)
          .writeCSV(s"$dir/Persons.txt")
        observationPeriods.filterPatients(patientIDs).toObservationPeriods.toDF.coalesce(1)
          .writeCSV(s"$dir/Observationperiods.txt")
        moleculeExposures.filterPatients(patientIDs).toDrugExposures.toDF.coalesce(1)
          .writeCSV(s"$dir/Drugexposures.txt")
        diseases.filterPatients(patientIDs).toConditionEras.toDF.coalesce(1)
          .writeCSV(s"$dir/Conditioneras.txt")

        rawPatients.unpersist()
      }

      diseases.unpersist()
      observationPeriods.unpersist()
      exposures.unpersist()
    }
  }
}
