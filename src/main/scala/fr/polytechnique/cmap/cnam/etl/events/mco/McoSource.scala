package fr.polytechnique.cmap.cnam.etl.events.mco

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import fr.polytechnique.cmap.cnam.etl.ColumnNames
import fr.polytechnique.cmap.cnam.etl.events.EventBuilder
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.{AssociatedDiagnosis, LinkedDiagnosis, MainDiagnosis}

trait McoSource extends ColumnNames {

  implicit class RichColName(colName: ColName) {
    def toCol: Column = col(colName)
  }

  final object ColNames {
    val PatientID: ColName = "NUM_ENQ"
    val DP: ColName = "`MCO_B.DGN_PAL`"
    val DR: ColName = "`MCO_B.DGN_REL`"
    val DA: ColName = "`MCO_D.ASS_DGN`"
    val CCAM: ColName = "`MCO_A.CDC_ACT`"
    val EtaNum: ColName = "ETA_NUM"
    val RsaNum: ColName = "RSA_NUM"
    val StayEndMonth: ColName = "`MCO_B.SOR_MOI`"
    val StayEndYear: ColName = "`MCO_B.SOR_ANN`"
    val StayLength: ColName = "`MCO_B.SEJ_NBJ`"
    val StayStartDate: ColName = "`ENT_DAT`"
    val StayEndDate: ColName = "`SOR_DAT`"
  }

  object NewColumns {
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

  private implicit class McoDataFrame(df: DataFrame) {

    /**
      * Estimate the stay starting date according to the different versions of PMSI MCO
      * Please note that in the case of early MCO (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {
      val cols = ColNames
      val dayInMs = 24L * 60 * 60
      val timeDelta: Column = coalesce(ColNames.StayLength.toCol, lit(0)) * dayInMs
      val estimate: Column = (ColNames.StayEndDate.toCol.cast(LongType) - timeDelta).cast(TimestampType)
      val roughEstimate: Column = (
        unix_timestamp(
          concat_ws("-", ColNames.StayEndYear.toCol, ColNames.StayEndMonth.toCol, lit("01 00:00:00"))
        ).cast(LongType) - timeDelta
        ).cast(TimestampType)

      df.withColumn(
        NewColumns.EstimatedStayStart,
        coalesce(ColNames.StayStartDate.toCol, estimate, roughEstimate)
      )
    }
  }

  def prepareDF(df: DataFrame): DataFrame = df.estimateStayStartTime
}
