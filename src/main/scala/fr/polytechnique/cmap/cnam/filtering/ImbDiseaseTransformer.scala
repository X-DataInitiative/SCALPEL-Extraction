package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._

/**
  * This transformer looks for CIM10 code starting with C67 in IR_IMB_R
  */
object ImbDiseaseTransformer extends DiseaseTransformer {

  val imbInputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("MED_NCL_IDT").as("imbEncoding"),
    col("MED_MTF_COD").as("disease"),
    col("IMB_ALD_DTD").as("eventDate")
  )

  implicit class imbDataFrame(df: DataFrame) {

    def extractImbDisease: DataFrame = {
      df.filter(col("imbEncoding") === "CIM10")
        .filter(col("disease") contains DiseaseCode)
    }

  }

  override def transform(sources: Sources): Dataset[Event] = {

    val irImb = sources.irImb.get

    import irImb.sqlContext.implicits._

    irImb.select(imbInputColumns: _*)
      .extractImbDisease
      .select(outputColumns: _*)
      .as[Event]

  }
}