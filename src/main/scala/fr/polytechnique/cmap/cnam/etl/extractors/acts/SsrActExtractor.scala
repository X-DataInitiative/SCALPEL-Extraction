package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, MedicalAct, SsrCCAMAct, SsrCSARRAct}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, StartsWithStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.ssr.SsrBasicExtractor

final case class SsrCcamActExtractor(codes: BaseExtractorCodes) extends SsrBasicExtractor[MedicalAct] with
  StartsWithStrategy[MedicalAct] {
  override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = SsrCCAMAct

  override def getCodes: BaseExtractorCodes = codes
}

/** Extract Csarr codes :
  *
  * The Specific Catalogue of Acts of Rehabilitation and Rehabilitation (CSARR) is intended to
  * describe and code the activity of the professionals concerned in follow-up care and
  * rehabilitation establishments (SSR). These acts are to be distinguished from CCAM acts which
  * are the sole responsibility of the doctor.
  *
  * This terminology is of the form `AAA+111`, eg. *GKQ+139 : Évaluation initiale du langage écrit*
  *
  * The complete terminology can be found here : https://drees.shinyapps.io/dico-snds/?variable=FP_PEC&search=csar&table=T_SSRaa_nnB
  * For more details see : https://www.atih.sante.fr/sites/default/files/public/content/3302/csarr_2018.pdf
  */
final case class SsrCsarrActExtractor(codes: BaseExtractorCodes) extends SsrBasicExtractor[MedicalAct] with
  StartsWithStrategy[MedicalAct] {
  override val columnName: String = ColNames.CSARR
  override val eventBuilder: EventBuilder = SsrCSARRAct

  override def getCodes: BaseExtractorCodes = codes
}