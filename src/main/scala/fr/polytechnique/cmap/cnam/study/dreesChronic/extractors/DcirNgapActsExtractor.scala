package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors


import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.{DcirNgapActExtractor, McoCeFbstcNgapActExtractor, McoCeFcstcNgapActExtractor, NgapActConfig}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets


class DcirNgapActsExtractor(ngapActConfig: NgapActConfig) {

  def extract(sources: Sources): Dataset[Event[NgapAct]] = {
    val dcirNgap = new DcirNgapActExtractor(ngapActConfig).extract(sources, Set.empty)
    val mcoFbstcNgap = new McoCeFbstcNgapActExtractor(ngapActConfig).extract(sources, Set.empty)
    val mcoFcstcNgap = new McoCeFcstcNgapActExtractor(ngapActConfig).extract(sources, Set.empty)

    unionDatasets(
      dcirNgap,
      mcoFbstcNgap,
      mcoFcstcNgap
    )
  }
}
