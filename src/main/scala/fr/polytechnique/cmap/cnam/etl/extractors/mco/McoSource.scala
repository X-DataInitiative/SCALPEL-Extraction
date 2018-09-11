package fr.polytechnique.cmap.cnam.etl.extractors.mco

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp

trait McoSource extends ColumnNames {

  final object ColNames extends Serializable {
    val PatientID: ColName = "NUM_ENQ"
    val DP: ColName = "MCO_B__DGN_PAL"
    val DR: ColName = "MCO_B__DGN_REL"
    val DA: ColName = "MCO_D__ASS_DGN"
    val CCAM: ColName = "MCO_A__CDC_ACT"
    val GHM: ColName = "MCO_B__GRG_GHM"
    val EtaNum: ColName = "ETA_NUM"
    val RsaNum: ColName = "RSA_NUM"
    val Year: ColName = "SOR_ANN"
    val StayEndMonth: ColName = "MCO_B__SOR_MOI"
    val StayEndYear: ColName = "MCO_B__SOR_ANN"
    val StayLength: ColName = "MCO_B__SEJ_NBJ"
    val StayStartDate: ColName = "ENT_DAT"
    val StayEndDate: ColName = "SOR_DAT"
  }

  object NewColumns extends Serializable {
    val EstimatedStayStart: ColName = "estimated_start"
  }

  final val eventBuilder: Map[ColName, EventBuilder] = Map(
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
