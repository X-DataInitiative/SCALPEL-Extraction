package fr.polytechnique.cmap.cnam.etl.events.diagnoses.old

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.{Diagnosis, ImbDiagnosis}

private[diagnoses] object ImbDiagnoses {

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("MED_NCL_IDT").as("encoding"),
    col("MED_MTF_COD").as("diagnosis_code"),
    col("IMB_ALD_DTD").cast(TimestampType).as("event_date")
  )

  def extract(config: ExtractionConfig, imb: DataFrame): Dataset[Event[Diagnosis]] = {
    import imb.sqlContext.implicits._

    val emptyWhen: Column = when(lit(false), "")
    val newCodeColumn: Column = config.imbDiagnosisCodes.foldLeft(emptyWhen) {
      (currentWhen, code) => currentWhen.when(col("diagnosis_code").startsWith(code), code)
    }

    imb.select(inputColumns: _*)
      .where(col("encoding") === "CIM10")
      .withColumn("diagnosis_code", newCodeColumn)
      .where(col("diagnosis_code").isNotNull)
      .where(col("event_date").isNotNull)
      .map(ImbDiagnosis.fromRow(_, "patientID", "diagnosis_code", "event_date"))
  }
}