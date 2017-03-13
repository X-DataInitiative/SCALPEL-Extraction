package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event

private[diagnoses] object McoDiagnoses {

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("ENT_DAT").cast("Timestamp").as("stay_start"),
    col("SOR_DAT").cast("Timestamp").as("stay_end"),
    col("`MCO_B.SOR_MOI`").as("stay_end_month"),
    col("`MCO_B.SOR_ANN`").as("stay_end_year"),
    col("`MCO_B.SEJ_NBJ`").as("stay_length"),
    col("`MCO_B.DGN_PAL`").as("main_diag"),
    col("`MCO_UM.DGN_PAL`").as("main_diag_um"),
    col("`MCO_B.DGN_REL`").as("linked_diag"),
    col("`MCO_UM.DGN_REL`").as("linked_diag_um"),
    col("`MCO_D.ASS_DGN`").as("associated_diag")
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
      val timeDelta: Column = coalesce(col("stay_length"), lit(0)) * dayInMs
      val estimate: Column = (col("stay_end").cast(LongType) - timeDelta).cast(TimestampType)
      val roughEstimate: Column = (
        unix_timestamp(
          concat_ws("-", col("stay_end_year"), col("stay_end_month"), lit("01 00:00:00"))
        ).cast(LongType) - timeDelta
      ).cast(TimestampType)

      df.withColumn("event_date", coalesce(col("stay_start"), estimate, roughEstimate)
      )
    }
  }

  /**
    * For each row, the columns in the `cols` parameter are searched for the codes in the `codes`
    * parameter. For each match, a new Diagnosis event is created using the `builder` parameter.
    *
    * Requires the column "eventDate" to be already computed.
    *
    * @return A function ready to be used by a DataFrame flat map
    */
  def rowToDiagnoses(
      cols: List[String],
      codes: List[String],
      builder: DiagnosisBuilder): (Row) => List[Event[Diagnosis]] = {

    (r: Row) => cols.flatMap(
      colName => {
        val idx = r.fieldIndex(colName)
        codes.collect {
          case code if !r.isNullAt(idx) && r.getString(idx).contains(code) =>
            builder(r.getAs[String]("patientID"), code, r.getAs[Timestamp]("event_date"))
        }
      }
    ).distinct
  }

  def extract(config: ExtractionConfig, mco: DataFrame): Dataset[Event[Diagnosis]] = {

    val mainDiagCodes: List[String] = config.mainDiagnosisCodes
    val mainDiagCols: List[String] = List("main_diag", "main_diag_um")

    val linkedDiagCodes: List[String] = config.linkedDiagnosisCodes
    val linkedDiagCols: List[String] = List("linked_diag","linked_diag_um")

    val assocDiagCodes: List[String] = config.associatedDiagnosisCodes
    val assocDiagCols: List[String] = List("associated_diag")

    val mcoWithDate = mco.select(inputColumns: _*).estimateStayStartTime

    import mco.sqlContext.implicits._
    List(
      mcoWithDate.flatMap(rowToDiagnoses(mainDiagCols, mainDiagCodes, MainDiagnosis)),
      mcoWithDate.flatMap(rowToDiagnoses(linkedDiagCols, linkedDiagCodes, LinkedDiagnosis)),
      mcoWithDate.flatMap(rowToDiagnoses(assocDiagCols, assocDiagCodes, AssociatedDiagnosis))
    ).reduce(_.union(_))
  }
}
