package fr.polytechnique.cmap.cnam.study.dreesChronic.extractors


import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.events.{Event, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts.{DcirNgapActExtractor, McoCeFbstcNgapActExtractor, McoCeFcstcNgapActExtractor, NgapActConfig, NgapWithNatClassConfig}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.unionDatasets


class DcirNgapActsExtractor(ngapActConfig: NgapActConfig[NgapWithNatClassConfig]) {

  def extract(sources: Sources): Dataset[Event[NgapAct]] = {
    val dcirNgap = DcirNgapActExtractor(ngapActConfig).extract(sources)
    val mcoFbstcNgap = McoCeFbstcNgapActExtractor(ngapActConfig).extract(sources)
    val mcoFcstcNgap = McoCeFcstcNgapActExtractor(ngapActConfig).extract(sources)

    unionDatasets(
      dcirNgap,
      mcoFbstcNgap,
      mcoFcstcNgap
    )
  }
}
