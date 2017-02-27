package fr.polytechnique.cmap.cnam.filtering.extraction

import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.Sources
import fr.polytechnique.cmap.cnam.filtering.events.{Event, Trackloss}

object Tracklosses extends EventsExtractor[Trackloss] {

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    coalesce(
      col("`ER_PHA_F.PHA_PRS_IDE`"),
      col("`ER_PHA_F.PHA_PRS_C13`")
    ).as("drug"),
    col("EXE_SOI_DTD").as("eventDate")
  )

  implicit class TracklossesDataFrame(data: DataFrame) {

    def withInterval(lastDate: Timestamp): DataFrame = {
      val window = Window.partitionBy(col("patientID")).orderBy(col("eventDate").asc)
      data
        .withColumn("nextDate", lead(col("eventDate"), 1, lastDate).over(window))
        .filter(col("nextDate").isNotNull)
        .withColumn("interval", months_between(col("nextDate"), col("eventDate")).cast("int"))
        .drop(col("nextDate"))
    }

    def filterTrackLosses(emptyMonths: Int): DataFrame = {
      data.filter(col("interval") >= emptyMonths)
    }

    def withTrackLossDate(tracklossMonthDelay: Int): DataFrame = {
      data.withColumn("tracklossDate", add_months(col("eventDate"), tracklossMonthDelay).cast(TimestampType))
    }
  }

  def extract(
      config: ExtractionConfig,
      sources: Sources): Dataset[Event[Trackloss]] = {

    val dcir: DataFrame = sources.dcir.get

    import dcir.sqlContext.implicits._
    dcir.select(inputColumns:_*)
      .filter(col("drug").isNotNull)
      .select(col("patientID"), col("eventDate"))
      .distinct
      .withInterval(config.lastDate)
      .filterTrackLosses(config.tracklossConfig.emptyMonths)
      .withTrackLossDate(config.tracklossConfig.tracklossMonthDelay)
      .map(Trackloss.fromRow(_, dateCol = "tracklossDate"))
      .toDS // todo: remove this on Spark 2
  }
}
