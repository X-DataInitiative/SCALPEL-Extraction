// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.acts


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, McoCEAct, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object McoCeActExtractor extends McoCeExtractor[MedicalAct] {
  val columnName: String = ColNames.CamCode
  override val eventBuilder: EventBuilder = McoCEAct

  override def getInput(sources: Sources): DataFrame = {
    sources.mcoCe.get.select((ColNames.CamCode :: ColNames.core).map(col): _*)
  }
}
