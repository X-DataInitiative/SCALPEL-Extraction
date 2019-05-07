package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

import fr.polytechnique.cmap.cnam.etl.events.{Event, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirSource
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class NgapActs(config: NgapActsConfig) extends NgapAct with DcirSource{

  def formatSource(sources: Sources, naturePrestation: Int): DataFrame = {
    sources.dcir.get.where(col(ColNames.NaturePrestation) === naturePrestation)
  }

  def extract(sources: Sources): Dataset[Event[NgapAct]] = {
    val dcirNgap: List[Dataset[Event[NgapAct]]] = config.acts_categories.map(
      ngapClass => DcirNgapActsExtractor(ngapClass.ngapCoefficients).extract(
        formatSource(sources, ngapClass.naturePrestation)
      )
    )

    dcirNgap.reduce(_.union(_)).distinct()
  }
}
