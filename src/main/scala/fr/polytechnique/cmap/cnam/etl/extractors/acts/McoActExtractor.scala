// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mco.McoExtractor

object McoCcamActExtractor extends McoExtractor[MedicalAct] {
  final override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = McoCCAMAct
}

object McoCimMedicalActExtractor extends McoExtractor[MedicalAct] {
  final override val columnName: String = ColNames.DP
  override val eventBuilder: EventBuilder = McoCIM10Act
}