package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ssr.SsrExtractor
import org.apache.spark.sql.Row

object SsrCcamActExtractor extends SsrExtractor[MedicalAct] {
  final override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = SsrCCAMAct
}

/** Extract Csarr codes :
  *
  *  The Specific Catalogue of Acts of Rehabilitation and Rehabilitation (CSARR) is intended to
  *  describe and code the activity of the professionals concerned in follow-up care and
  *  rehabilitation establishments (SSR). These acts are to be distinguished from CCAM acts which
  *  are the sole responsibility of the doctor.
  *
  *  This terminology is of the form `AAA+111`, eg. *GKQ+139 : Évaluation initiale du langage écrit*
  *
  *  The complete terminology can be found here : https://drees.shinyapps.io/dico-snds/?variable=FP_PEC&search=csar&table=T_SSRaa_nnB
  *  For more details see : https://www.atih.sante.fr/sites/default/files/public/content/3302/csarr_2018.pdf
  */
object SsrCsarrActExtractor extends SsrExtractor[MedicalAct] {
  final override val columnName: String = ColNames.CSARR
  override val eventBuilder: EventBuilder = SsrCSARRAct
}