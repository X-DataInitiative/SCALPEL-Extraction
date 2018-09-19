package fr.polytechnique.cmap.cnam.study.fall

import java.sql.Timestamp
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, Outcome}
import fr.polytechnique.cmap.cnam.util.datetime.implicits._

private[fall] class FracturesTransformerImplicits(outcomes: Dataset[Event[Outcome]]) extends Serializable {


  def groupConsecutiveFractures(frame: Period): Dataset[Event[Outcome]] = {

    val sqlCtx = outcomes.sqlContext
    import sqlCtx.implicits._
    outcomes.groupByKey(outcome => (outcome.patientID, outcome.groupID))
      .flatMapGroups { case (_, outcomesGrouped) =>
        val outcomesStream = outcomesGrouped.toStream
        val outcomeDates = outcomesStream.map(_.start)
          .sorted

        val filteredDates = outcomeDates.foldLeft(List(): List[(Timestamp, Boolean)]) {
          case (outcomeDates, date) =>
            if (outcomeDates.isEmpty || outcomeDates.head._1.+(frame).get.before(date))
              (date, true) :: outcomeDates
            else (date, false) :: outcomeDates
        }.filter(_._2).map(_._1)
        outcomesStream.filter(outcome => filteredDates.contains(outcome.start))

      }
  }

}

object FracturesTransformerImplicits {
  implicit def addFracturesImplicits(outcomes: Dataset[Event[Outcome]]): FracturesTransformerImplicits = {
    new FracturesTransformerImplicits(outcomes)
  }
}
