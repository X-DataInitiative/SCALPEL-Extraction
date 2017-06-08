package fr.polytechnique.cmap.cnam.etl.events.diagnoses.old

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.{AssociatedDiagnosis, Diagnosis, LinkedDiagnosis, MainDiagnosis}

private[diagnoses] object McoDiagnoses {

  private object McoColumns {
    val patientID: Column = col("NUM_ENQ")
    val stayStart: Column = col("ENT_DAT").cast("Timestamp")
    val stayEnd: Column = col("SOR_DAT").cast("Timestamp")
    val stayEndMonth: Column = col("`MCO_B.SOR_MOI`")
    val stayEndYear: Column = col("`MCO_B.SOR_ANN`")
    val stayLength: Column = col("`MCO_B.SEJ_NBJ`")
    val mainDiagnosisCodes: Column = col("`MCO_B.DGN_PAL`")
    val linkedDiagnosisCodes: Column = col("`MCO_B.DGN_REL`")
    val associatedDiagnosisCodes: Column = col("`MCO_D.ASS_DGN`")
  }

  val inputColumns: List[Column] = {
    import McoColumns._
    List(patientID, stayStart, stayEnd, stayEndMonth, stayEndYear, stayLength, mainDiagnosisCodes,
      linkedDiagnosisCodes, associatedDiagnosisCodes)
  }

  implicit class McoDataFrame(df: DataFrame) {

    /**
      * Estimate the stay starting date according to the different versions of PMSI MCO
      * Please note that in the case of early MCO (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {
      val cols = McoColumns
      val dayInMs = 24L * 60 * 60
      val timeDelta: Column = coalesce(cols.stayLength, lit(0)) * dayInMs
      val estimate: Column = (cols.stayEnd.cast(LongType) - timeDelta).cast(TimestampType)
      val roughEstimate: Column = (
        unix_timestamp(
          concat_ws("-", cols.stayEndYear, cols.stayEndMonth, lit("01 00:00:00"))
        ).cast(LongType) - timeDelta
      ).cast(TimestampType)

      df.withColumn("eventDate", coalesce(cols.stayStart, estimate, roughEstimate))
    }
  }

  case class McoDiagnosesCodes (
    patientID: String,
    eventDate: Timestamp,
    mainDiagnosisCode: Option[String],
    linkedDiagnosisCode: Option[String],
    associatedDiagnosisCode: Option[String]
  )

  def findCodesInColumn(column: Column, codes: List[String]): Column = {
    val emptyWhen: Column = when(lit(false), "")
    codes.foldLeft(emptyWhen) {
     (currentWhen, code) => currentWhen.when(column.startsWith(code), code)
    }
  }

  def buildDiagnoses: (McoDiagnosesCodes) => List[Event[Diagnosis]] = {
    (codes: McoDiagnosesCodes) => {
      codes.mainDiagnosisCode.map(MainDiagnosis(codes.patientID, _, codes.eventDate)) ++
      codes.linkedDiagnosisCode.map(LinkedDiagnosis(codes.patientID, _, codes.eventDate)) ++
      codes.associatedDiagnosisCode.map(AssociatedDiagnosis(codes.patientID, _, codes.eventDate))
    }.toList
  }

  def extract(config: ExtractionConfig, mco: DataFrame): Dataset[Event[Diagnosis]] = {

    import mco.sqlContext.implicits._

    val mainDiagCodes: List[String] = config.mainDiagnosisCodes
    val linkedDiagCodes: List[String] = config.linkedDiagnosisCodes
    val assocDiagCodes: List[String] = config.associatedDiagnosisCodes

    mco
      .select(inputColumns: _*)
      .estimateStayStartTime
      .withColumn("mainDiagnosisCode", findCodesInColumn(McoColumns.mainDiagnosisCodes, mainDiagCodes))
      .withColumn("linkedDiagnosisCode", findCodesInColumn(McoColumns.linkedDiagnosisCodes, linkedDiagCodes))
      .withColumn("associatedDiagnosisCode", findCodesInColumn(McoColumns.associatedDiagnosisCodes, assocDiagCodes))
      .withColumn("patientID", McoColumns.patientID)
      .as[McoDiagnosesCodes]
      .flatMap(buildDiagnoses)
  }
}
