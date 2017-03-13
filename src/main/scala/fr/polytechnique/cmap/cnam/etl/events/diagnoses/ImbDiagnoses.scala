package fr.polytechnique.cmap.cnam.etl.events.diagnoses

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event

private[diagnoses] object ImbDiagnoses {

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("MED_NCL_IDT").as("encoding"),
    col("MED_MTF_COD").as("diagnosis_code"),
    col("IMB_ALD_DTD").cast(TimestampType).as("event_date")
  )

  /**
    * todo: This method can probably be merged with McoDiagnoses.rowToDiagnoses, passing
    * ImbDiagnosis as third parameter. However, it is necessary to think about the best organization
    */
  def rowToDiagnoses(codes: List[String]): (Row) => List[Event[Diagnosis]] = {
    (r: Row) => {
      val i = r.fieldIndex("diagnosis_code")
      codes.collect {
        case code if !r.isNullAt(i) && r.getString(i).contains(code) =>
        ImbDiagnosis(r.getAs[String]("patientID"), code, r.getAs[Timestamp]("event_date"))
      }
    }
  }

  def extract(config: ExtractionConfig, imb: DataFrame): Dataset[Event[Diagnosis]] = {
    import imb.sqlContext.implicits._
    imb.select(inputColumns: _*)
      .where(col("encoding") === "CIM10")
      .where(col("event_date").isNotNull)
      .flatMap(rowToDiagnoses(config.imbDiagnosisCodes))
  }
}