// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.tracklosses

import me.danielpes.spark.datetime.implicits._
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Dispensation, Event, Trackloss}
import fr.polytechnique.cmap.cnam.util.functions._

class TracklossTransformer(config: TracklossesConfig) extends Serializable {

  def transform[T <: Dispensation](drugs: Dataset[Event[T]]): Dataset[Event[Trackloss]] = {

    val sqlCtx = drugs.sqlContext
    import sqlCtx.implicits._

    drugs.groupByKey(_.patientID).flatMapGroups((_, events) => fromDispensationToTrackloss(events)).distinct()

  }

  private def fromDispensationToTrackloss(events: Iterator[Event[Dispensation]]): TraversableOnce[Event[Trackloss]] = {

    val sortedEvents = events.toList.sortBy(_.start)
    val lastEvent: Event[Dispensation] = sortedEvents.last.copy(start = config.studyEnd)

    val addMonthDelay = (event: Event[Dispensation]) => Trackloss(event.patientID, (event.start + config.tracklossMonthDelay).get)

    (sortedEvents :+ lastEvent).toStream.sliding(2, 1).filter(isInSlide).map(slide => addMonthDelay(slide.head))
  }

  private def isInSlide(slide: Stream[Event[Dispensation]]): Boolean = {
    val reachTS = (slide.head.start + config.emptyMonths).get
    slide.last.start.after(reachTS) || slide.last.start.equals(reachTS)
  }


}
