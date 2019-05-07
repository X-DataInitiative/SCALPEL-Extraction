package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors


import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{NgapAct, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.{NgapActConfig, DcirNgapActExtractor}
import fr.polytechnique.cmap.cnam.etl.sources.Sources


class DcirNgapActsExtractor(ngapActConfig: NgapActConfig) {

  def extract(sources: Sources): Dataset[Event[NgapAct]] = {
    new DcirNgapActExtractor(ngapActConfig).extract(sources, Set.empty)
  }
}
