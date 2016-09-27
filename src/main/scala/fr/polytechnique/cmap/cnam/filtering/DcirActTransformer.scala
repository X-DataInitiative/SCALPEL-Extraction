package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


object DcirActTransformer extends Transformer[Event] {

  val ActCode = "Z511"

  val inputColumns = List(
    col("NUM_ENQ").as("patientID"),
    col("`ER_CAM_F.CAM_PRS_IDE`").as("actCode"),
    col("EXE_SOI_DTD").as("eventDate")
  )

  val outputColumns = List(
    col("patientID"),
    lit("act").as("category"),
    lit("radiotherapy").as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class ActDF(data: DataFrame) {
      def filterActs: DataFrame = {
        data.filter(col("actCode") === ActCode)
      }
  }


  override def transform(sources: Sources): Dataset[Event] = {
    val dcir = sources.dcir.get
    import dcir.sqlContext.implicits._

    dcir.select(inputColumns:_*)
      .filterActs
      .select(outputColumns: _*)
      .as[Event]
  }
}
