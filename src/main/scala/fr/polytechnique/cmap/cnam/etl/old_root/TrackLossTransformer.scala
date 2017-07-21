package fr.polytechnique.cmap.cnam.etl.old_root

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources


object TrackLossTransformer extends Transformer[Event]{

  val EmptyMonths: Int = FilteringConfig.tracklossDefinition.threshold
  val TracklossMonthDelay: Int = FilteringConfig.tracklossDefinition.delay
  val LastDay: Timestamp = FilteringConfig.dates.studyEnd

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    coalesce(
      col("ER_PHA_F__PHA_PRS_IDE"),
      col("ER_PHA_F__PHA_PRS_C13")
    ).as("drug"),
    col("EXE_SOI_DTD").as("eventDate")
  )
  val outputColumns: List[Column] = List(
    col("patientID").as("patientID"),
    lit("trackloss").as("category"),
    lit("eventId").as("eventId"),
    lit(1.0).as("weight"),
    col("trackloss").as("start"),
    lit(null).cast(TimestampType).as("end")
  )
  implicit class TrackLossDataFrame(data: DataFrame) {


    def withInterval: DataFrame = {

      val window = Window.partitionBy(col("patientID")).orderBy(col("eventDate").asc)

      data
        .withColumn("nextDate", lead(col("eventDate"), 1, LastDay).over(window))
        .filter(col("nextDate").isNotNull)
        .withColumn("interval", months_between(col("nextDate"), col("eventDate")).cast("int"))
        .drop(col("nextDate"))
    }

    def filterTrackLosses: DataFrame = {
      data.filter(col("interval") >= EmptyMonths)
    }
    def withTrackLossDate: DataFrame = {
      data.withColumn("trackloss", add_months(col("eventDate"), TracklossMonthDelay).cast(TimestampType))
    }

  }

  override def transform(sources: Sources): Dataset[Event] = {
    val dcir: DataFrame = sources.dcir.get
    import dcir.sqlContext.implicits._

    dcir.select(inputColumns:_*)
      .filter(col("drug").isNotNull)
      .select(col("patientID"), col("eventDate"))
      .distinct
      .withInterval
      .filterTrackLosses
      .withTrackLossDate
      .select(outputColumns: _*)
      .as[Event]
  }
}
