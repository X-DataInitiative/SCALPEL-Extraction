// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.extractors._
import fr.polytechnique.cmap.cnam.util.reporting._
import org.apache.spark.sql.{Encoders, SparkSession}

class FallMainExtractorTransformSuite extends SharedContext {

  "ExtractionSerialization" should "deserialize and work" in {
    val sqlCtx = sqlContext
    val spark = SparkSession.builder.getOrCreate()
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
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    assertDSs(new DiagnosisExtractor(fallConfig.diagnoses).extract(sources),
      spark.read.parquet(meta.get("diagnoses").get.outputPath)
        .as(Encoders.bean(classOf[Event[Diagnosis]])))
    assertDSs(new ActsExtractor(fallConfig.medicalActs).extract(sources)._1,
      spark.read.parquet(meta.get("acts").get.outputPath)
        .as(Encoders.bean(classOf[Event[MedicalAct]])))
    assertDSs(new Patients(PatientsConfig(fallConfig.base.studyStart)).extract(sources),
      spark.read.parquet(meta.get("extract_patients").get.outputPath)
        .as(Encoders.bean(classOf[Patient])))
  }
}
