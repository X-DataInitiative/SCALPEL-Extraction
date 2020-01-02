package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.had.HadExtractor
import org.apache.spark.sql.Row

object HadCcamActExtractor extends HadExtractor[MedicalAct] {
  final override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = HadCCAMAct
}


