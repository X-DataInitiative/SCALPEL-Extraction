package fr.polytechnique.cmap.cnam.etl.extractors.mco

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp

@deprecated("I said so")
trait McoSource extends ColumnNames {

  final object ColNames extends Serializable {
    final lazy val PatientID = "NUM_ENQ"
    final lazy val DP = "MCO_B__DGN_PAL"
    final lazy val DR = "MCO_B__DGN_REL"
    final lazy val DA = "MCO_D__ASS_DGN"
    final lazy val CCAM = "MCO_A__CDC_ACT"
    final lazy val GHM = "MCO_B__GRG_GHM"
    final lazy val EtaNum = "ETA_NUM"
    final lazy val RsaNum = "RSA_NUM"
    final lazy val Year = "SOR_ANN"
    final lazy val StayEndMonth = "MCO_B__SOR_MOI"
    final lazy val StayEndYear = "MCO_B__SOR_ANN"
    final lazy val StayLength = "MCO_B__SEJ_NBJ"
    final lazy val StayStartDate = "ENT_DAT"
    final lazy val StayEndDate = "SOR_DAT"
  }

  object NewColumns extends Serializable {
    val EstimatedStayStart = "estimated_start"
  }

  final val eventBuilder: Map[ColName, Diagnosis] = Map(
    ColNames.DP -> MainDiagnosis,
    ColNames.DR -> LinkedDiagnosis,
    ColNames.DA -> AssociatedDiagnosis
  )

  val colNameFromConfig: Map[String, ColName] = Map(
    "dp" -> ColNames.DP,
    "dr" -> ColNames.DR,
    "da" -> ColNames.DA
  )

  implicit class McoDataFrame(df: DataFrame) {

    /**
      * Estimate the stay starting date according to the different versions of PMSI MCO
      * Please note that in the case of early MCO (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {
      val dayInMs = 24L * 60 * 60
      val timeDelta: Column = coalesce(ColNames.StayLength.toCol, lit(0)) * dayInMs
      val estimate: Column = {
        val endDate = parseTimestamp(ColNames.StayEndDate.toCol, "ddMMyyyy")
        (endDate.cast(LongType) - timeDelta).cast(TimestampType)
      }
      val roughEstimate: Column = (
        unix_timestamp(
          concat_ws("-", ColNames.StayEndYear.toCol, ColNames.StayEndMonth.toCol, lit("01 00:00:00"))
        ).cast(LongType) - timeDelta
      ).cast(TimestampType)

      val givenDate: Column = parseTimestamp(ColNames.StayStartDate.toCol, "ddMMyyyy")

      df.withColumn(
        NewColumns.EstimatedStayStart,
        coalesce(givenDate, estimate, roughEstimate)
      )
    }
  }

  def estimateStayStartTime(mco: DataFrame): DataFrame = mco.estimateStayStartTime
}
