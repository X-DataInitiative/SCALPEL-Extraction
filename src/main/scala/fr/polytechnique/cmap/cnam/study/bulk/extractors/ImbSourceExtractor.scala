// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors

import fr.polytechnique.cmap.cnam.etl.events.Diagnosis
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses.ImbDiagnosisExtractor

class ImbSourceExtractor(override val path: String, override val saveMode: String) extends SourceExtractor(
  path,
  saveMode
) {
  override val sourceName: String = "IMB_R"
  override val extractors = List(
    ExtractorSources[Diagnosis](ImbDiagnosisExtractor, List("IR_IMB_R"), "ALD")
  )
}
