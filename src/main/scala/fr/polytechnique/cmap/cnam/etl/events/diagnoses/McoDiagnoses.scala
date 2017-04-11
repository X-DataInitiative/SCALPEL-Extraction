package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.util.McoUtils._

private[diagnoses] object McoDiagnoses {

  private object McoColumns extends McoTimeColumns {
    val patientID: Column = col("NUM_ENQ")
    val mainDiagnosisCodes: Column = col("`MCO_B.DGN_PAL`")
    val linkedDiagnosisCodes: Column = col("`MCO_B.DGN_REL`")
    val associatedDiagnosisCodes: Column = col("`MCO_D.ASS_DGN`")
  }

  val inputColumns: List[Column] = {
    import McoColumns._
    List(patientID, stayStart, stayEnd, stayEndMonth, stayEndYear, stayLength, mainDiagnosisCodes,
      linkedDiagnosisCodes, associatedDiagnosisCodes)
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
