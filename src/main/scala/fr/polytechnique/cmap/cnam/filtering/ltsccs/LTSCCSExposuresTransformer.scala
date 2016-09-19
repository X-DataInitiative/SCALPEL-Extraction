package fr.polytechnique.cmap.cnam.filtering.ltsccs

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.filtering.{ExposuresTransformer, FlatEvent}

object LTSCCSExposuresTransformer extends ExposuresTransformer {

  val inputColumns = Seq(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    col("eventId").as("moleculeName"),
    col("start").as("eventDate"),
    col("end")
  )

  val outputColumns = List(
    col("patientID"),
    col("gender"),
    col("birthDate"),
    col("deathDate"),
    lit("exposure").as("category"),
    col("moleculeName").as("eventId"),
    lit(1.0).as("weight"),
    col("exposureStart").as("start"),
    col("exposureEnd").as("end")
  )

  implicit class LTSCCSExposuresDataFrame(data: DataFrame) {

    final val ExposureStartThreshold = 6
    final val ExposureEndThreshold = 4

    val window = Window.partitionBy("patientID", "moleculeName")
    val orderedWin = window.orderBy("eventDate")

    def withNextDate: DataFrame = data.withColumn("nextDate", lead(col("eventDate"), 1).over(orderedWin))
    def withDelta: DataFrame = data.withColumn("delta", months_between(col("nextDate"), col("eventDate")))

    def filterPatients: DataFrame = data

    def getTracklosses: DataFrame = {
      data
        .withColumn("rank", row_number().over(orderedWin)) // This is used to find the first line of the window
        .where(
          col("nextDate").isNull ||   // The last line of the ordered window (lead("eventDate") == null)
          (col("rank") === 1) ||      // The first line of the ordered window
          (col("delta") > ExposureEndThreshold)  // All the lines that represent a trackloss
        )
        .select(col("patientID"), col("moleculeName"), col("eventDate"))
        .withColumn("tracklossDate", lead(col("eventDate"), 1).over(orderedWin))
        .where(col("tracklossDate").isNotNull)
    }

    def withExposureEnd(tracklosses: DataFrame): DataFrame = {

      // I needed this redefinition of names because I was getting some very weird errors when using
      //   the .as() function and .select("table.*")
      val adjustedTracklosses = tracklosses.select(
        col("patientID").as("t_patientID"),
        col("moleculeName").as("t_moleculeName"),
        col("eventDate").as("t_eventDate"),
        col("tracklossDate")
      )

      // For every row in the tracklosses DataFrame, we will take the trackloss date and add it as
      //   the exposureEnd date for every purchase that happened between the previous trackloss and
      //   the current one.
      val joinConditions =
        (col("patientID") === col("t_patientID")) &&
        (col("moleculeName") === col("t_moleculeName")) &&
        (col("eventDate") >= col("t_eventDate")) &&
        (col("eventDate") < col("tracklossDate"))

      data
        .join(adjustedTracklosses, joinConditions, "left_outer")
        .withColumnRenamed("tracklossDate", "exposureEnd")
    }

      def withExposureStart: DataFrame = {
      val window = Window.partitionBy("patientID", "moleculeName", "exposureEnd")

      // We take the first pair of purchases that happened within the threshold and set the
      //   the exposureStart date as the date of the second purchase of the pair.
      val adjustedNextDate: Column = when(col("delta") <= ExposureStartThreshold, col("nextDate"))
      data.withColumn("exposureStart", min(adjustedNextDate).over(window))
    }

  }

  override def transform(input: Dataset[FlatEvent]): Dataset[FlatEvent] = {
    import input.sqlContext.implicits._

    val events = input.toDF
      .filterPatients
      .where(col("category") === "molecule")
      .select(inputColumns: _*)
      .repartition(col("patientID"), col("moleculeName"))
      .persist()

    val eventsWithDelta = events
      .withNextDate
      .withDelta

    val tracklosses = eventsWithDelta.getTracklosses

    val result = eventsWithDelta
      .withExposureEnd(tracklosses)
      .withExposureStart
      .where(col("exposureEnd") > col("exposureStart"))
      .select(outputColumns: _*)
      .dropDuplicates(Seq("patientID", "eventId", "start", "end"))
      .as[FlatEvent]

    events.unpersist()
    result
  }
}
