package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/** This transformer looks for CIM10 code starting with C67 in IR_IMB_R
  *
  */
object CancerTransformer extends Transformer[Event] {
  private final val EncodingColumn: String = "MED_NCL_IDT"
  private final val DiagnoseColumn: String = "MED_MTF_COD"
  private final val PatientIDColumn: String = "NUM_ENQ"
  private final val StartTimeColumn: String = "IMB_ALD_DTD"
  private final val CancerCode: String = "C67"

  val inputColumns: List[Column] = List(
    col(PatientIDColumn),
    col(EncodingColumn),
    col(DiagnoseColumn),
    col(StartTimeColumn)
  )

  val outputColumns: List[Column] = List(
    col(PatientIDColumn).as("patientID"),
    lit("disease").as("category"),
    lit("C67").as("eventId"),
    lit(1.00).as("weight"),
    col(StartTimeColumn).cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class CancerDataFrame(df: DataFrame) {

    def extractCancer: DataFrame = {
      df.filter(col(EncodingColumn) === "CIM10")
        .filter(col(DiagnoseColumn) contains CancerCode)
    }
  }

  override def transform(sources: Sources): Dataset[Event] = {

    val irImbR: DataFrame = sources.irImb.get
    import irImbR.sqlContext.implicits._

    irImbR.select(inputColumns:_*)
      .extractCancer
      .select(outputColumns:_*)
      .as[Event]
  }
}
