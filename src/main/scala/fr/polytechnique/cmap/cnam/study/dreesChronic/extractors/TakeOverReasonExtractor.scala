package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors

import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, MedicalTakeOverReason}
import fr.polytechnique.cmap.cnam.etl.extractors.takeOverReasons.{HadAssociatedTakeOverExtractor, HadMainTakeOverExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets
import org.apache.spark.sql.Dataset

class TakeOverReasonExtractor {

  def extract(sources: Sources): Dataset[Event[MedicalTakeOverReason]] = {

    val hadMainTakeOverReason = HadMainTakeOverExtractor.extract(sources, Set.empty)
    val hadAssociatedTakeOverReason = HadAssociatedTakeOverExtractor.extract(sources, Set.empty)

    unionDatasets(
      hadMainTakeOverReason,
      hadAssociatedTakeOverReason
    )
  }
}
