package fr.polytechnique.cmap.cnam.study.fall.fractures

import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.etl.events.{Event, Outcome}

private[fall] class FracturesTransformerImplicits(outcomes: Dataset[Event[Outcome]]) extends Serializable {

  /**
    * The fracture only depends on site and patient
    * When we have multiple fracture events of the same site happening consecutively within a period,
    * they should be considered as a single one, with the date equal to the first event of the period.
    *
    * @param frame the followup period
    * @return a dateset of fracture events
    */
  def groupConsecutiveFractures(frame: Period): Dataset[Event[Outcome]] = {

    val sqlCtx = outcomes.sqlContext
    import sqlCtx.implicits._

    val window = Window.partitionBy("patientID", "groupID").orderBy("start")

    val statusCol = coalesce(
      when(col("lastStart").isNull, "first"),
      when(col("start") >= col("lastStart").addPeriod(frame), "first")
    )

    outcomes
      .withColumn("lastStart", lag("start", 1).over(window))
      .withColumn("status", statusCol)
      .filter(col("status") === "first")
      .drop("lastStart", "status")
      .as[Event[Outcome]]
      .distinct
  }


}

object FracturesTransformerImplicits {
  implicit def addFracturesImplicits(outcomes: Dataset[Event[Outcome]]): FracturesTransformerImplicits = {
    new FracturesTransformerImplicits(outcomes)
  }
}
