package fr.polytechnique.cmap.cnam.study.fall.statistics

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}

trait Statistical

trait EventStatistics[A <: AnyEvent, B <: Statistical] {
  def process(ds: Dataset[Event[A]]): Dataset[B]
}


