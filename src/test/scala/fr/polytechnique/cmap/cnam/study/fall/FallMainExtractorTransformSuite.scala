// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall

import org.apache.spark.sql.Encoders
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{AllPatientExtractor, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.sources.general.GeneralSource
import fr.polytechnique.cmap.cnam.etl.transformers.patients.PatientFilters
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.extractors._
import fr.polytechnique.cmap.cnam.util.reporting._

class FallMainExtractorTransformSuite extends SharedContext {

  "ExtractionSerialization" should "deserialize and work" in {
    val sqlCtx = sqlContext
    import implicits.SourceReader
    val params = Map(
      "conf" -> "/src/main/resources/config/fall/default.conf",
      "env" -> "test", // change "test" to "save" for python interop
      "meta_bin" -> "target/test/output/meta_data.bin"
    )
    FallMainExtract.run(sqlCtx, params)
    FallMainTransform.run(sqlCtx, params)

    val fallConfig = FallConfig.load(params("conf"), params("env"))
    val meta = OperationMetadata.deserialize(params("meta_bin"))
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input, fallConfig.readFileFormat))
    assertDSs(new DiagnosisExtractor(fallConfig.diagnoses).extract(sources),
      GeneralSource.read(sqlCtx, meta.get("diagnoses").get.outputPath, fallConfig.readFileFormat)
        .as(Encoders.bean(classOf[Event[Diagnosis]])))
    assertDSs(new ActsExtractor(fallConfig.medicalActs).extract(sources)._1,
      GeneralSource.read(sqlCtx, meta.get("acts").get.outputPath, fallConfig.readFileFormat)
        .as(Encoders.bean(classOf[Event[MedicalAct]])))
    assertDSs(new PatientFilters(PatientsConfig(fallConfig.base.studyStart)).filterPatients(AllPatientExtractor.extract(sources)),
      GeneralSource.read(sqlCtx, meta.get("extract_filtered_patients").get.outputPath, fallConfig.readFileFormat)
        .as(Encoders.bean(classOf[Patient])))
  }
}
