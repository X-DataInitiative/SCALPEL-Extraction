package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.NewMcoExtractor

object NewMcoCcamActExtractor extends NewMcoExtractor[MedicalAct]{
  final override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = McoCCAMAct
}

object NewMcoCimMedicalActExtractor extends NewMcoExtractor[MedicalAct] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = McoCIM10Act
}