// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, McoCeFbstcMedicalPractitionerClaim, McoCeFcstcMedicalPractitionerClaim, PractitionerClaimSpeciality}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, IsInStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeBasicExtractor

/**
  * Get specialties of the non medical practitioners in the MCO_CE:
  * If a specialty is available, it extracts the specialty using MCO_FBSTC_ _EXE_SPE and MCO_FCSTC_ _EXE_SPE.
  * These two columns are complementary as described here :
  * https://documentation-snds.health-data-hub.fr/fiches/actes_consult_externes.html#les-tables-du-pmsi-version-snds-pour-les-ace
  **/
sealed abstract class McoCeSpecialtyExtractor(codes: BaseExtractorCodes) extends McoCeBasicExtractor[PractitionerClaimSpeciality]
  with IsInStrategy[PractitionerClaimSpeciality] {
  override def extractValue(row: Row): String = row.getAs[Int](columnName).toString

  override def isInExtractorScope(row: Row): Boolean = {
    (!row.isNullAt(row.fieldIndex(columnName))) & (row.getAs[Integer](columnName) != 0)
  }

  override def getCodes: BaseExtractorCodes = codes
}

final case class McoCeFbstcSpecialtyExtractor(codes: BaseExtractorCodes) extends McoCeSpecialtyExtractor(codes) {
  override val columnName: String = ColNames.PractitionnerSpecialtyFbstc
  override val eventBuilder: EventBuilder = McoCeFbstcMedicalPractitionerClaim
}


final case class McoCeFcstcSpecialtyExtractor(codes: BaseExtractorCodes) extends McoCeSpecialtyExtractor(codes) {
  override val columnName: String = ColNames.PractitionnerSpecialtyFcstc
  override val eventBuilder: EventBuilder = McoCeFcstcMedicalPractitionerClaim
}
