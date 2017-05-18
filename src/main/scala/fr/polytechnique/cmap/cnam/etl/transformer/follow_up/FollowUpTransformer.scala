package fr.polytechnique.cmap.cnam.etl.transformer.follow_up


import fr.polytechnique.cmap.cnam.etl.events.Event
import fr.polytechnique.cmap.cnam.etl.events.molecules.Molecule
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.transformer.exposure.ExposuresConfig
import fr.polytechnique.cmap.cnam.etl.transformer.observation._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.util.ColumnUtilities._
import fr.polytechnique.cmap.cnam.util.RichDataFrames._

class FollowUpTransformer(val delay: Int, val diseaseCode: String) {

  val outputColumns = List(
    col("patientID"),
    col("followUpStart").as("start"),
    col("followUpEnd").as("stop")
  )


  def transform(
      patients: Dataset[(Patient, ObservationPeriod)],
      dispensations: Dataset[Event[Molecule]])
    : Dataset[FollowUp] = {

    import FollowUpTransformer._

    val inputCols = Seq(
      col("Patient.patientID").as("patientID"),
      col("Patient.gender").as("gender"),
      col("Patient.birthDate").as("birthDate"),
      col("Patient.deathDate").as("deathDate"),
      col("ObservationPeriod.start").as("observationStart"),
      col("ObservationPeriod.stop").as("observationEnd")
    )

    val input = renameTupleColumns(patients)
      .select(inputCols:_*)
      .join(dispensations, Seq("patientID"))

    import input.sqlContext.implicits._

    val events = input.repartition(col("patientID"))

    events
      .withFollowUpStart(delay)
      .withTrackloss
      .withFollowUpEnd
      .na.drop("any", Seq("followUpStart", "followUpEnd"))
      .withEndReason
      .select(outputColumns: _*)
      .dropDuplicates(Seq("PatientID"))
      .as[FollowUp]
  }
}

object FollowUpTransformer {

  def apply(config: ExposuresConfig): FollowUpTransformer = {
    val followUpMonthsDelay: Int = config.followUpDelay
    val diseaseCode: String = config.diseaseCode
    new FollowUpTransformer(followUpMonthsDelay, diseaseCode)
  }

  implicit class FollowUpDataFrame(data: DataFrame) {

    def withFollowUpStart(followUpMonthsDelay: Int): DataFrame = {
      val window = Window.partitionBy("patientID")

      val followUpStart = add_months(col("observationStart"), followUpMonthsDelay).cast(TimestampType)
      val correctedFollowUpStart = when(followUpStart < col("observationEnd"), followUpStart)

      data.withColumn("followUpStart", min(correctedFollowUpStart).over(window))
    }

    def withTrackloss: DataFrame = {
      val window = Window.partitionBy("patientID")

      val firstCorrectTrackloss = min(
        when(col("category") === "trackloss" && (col("start") > col("followUpStart")), col("start"))
      ).over(window)

      data.withColumn("trackloss", firstCorrectTrackloss)
    }

    def withFollowUpEnd: DataFrame = {
      val window = Window.partitionBy("patientID")

      val firstTargetDisease = min(
        when(col("category") === "disease" && col("value") === "targetDisease", col("start"))
      ).over(window)

      data
        .withColumn("firstTargetDisease", firstTargetDisease)
        .withColumn("followUpEnd",
          minColumn(
            col("deathDate"),
            col("firstTargetDisease"),
            col("trackloss"),
            col("observationEnd")
          )
        )
    }

    def withEndReason: DataFrame = {
      val endReason = when(
        col("followUpEnd") === col("deathDate"), "death"
      ).when(
        col("followUpEnd") === col("firstTargetDisease"), "disease"
      ).when(
        col("followUpEnd") === col("trackloss"), "trackloss"
      ).when(
        col("followUpEnd") === col("observationEnd"), "observationEnd"
      )

      data.withColumn("endReason", endReason)
    }
  }
}
