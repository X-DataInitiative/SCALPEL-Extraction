package fr.polytechnique.cmap.cnam.study.fall
import java.io._
import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession, Encoders}
import org.apache.spark.sql.functions.lit
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays.HospitalStaysExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.filters.PatientFilters
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.etl.transformers.exposures.ExposuresTransformer
import fr.polytechnique.cmap.cnam.study.fall.codes._
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.study.fall.extractors._
import fr.polytechnique.cmap.cnam.study.fall.follow_up.FallStudyFollowUps
import fr.polytechnique.cmap.cnam.study.fall.fractures.FracturesTransformer
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.reporting._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.{Cip13Level, MoleculeCombinationLevel, PharmacologicalLevel, TherapeuticLevel}
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import fr.polytechnique.cmap.cnam.etl.transformers.follow_up.FollowUp
import fr.polytechnique.cmap.cnam.util.reporting._

class FallMainExtractorTransformSuite extends SharedContext {

  "ExtractionSerialization" should "deserialize and work" in {
    val sqlCtx = sqlContext
    val spark =  SparkSession.builder.getOrCreate()
    import sqlCtx.implicits._
    import spark.implicits._
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
    assertDSs(new ActsExtractor(fallConfig.medicalActs).extract(sources),
               spark.read.parquet(meta.get("acts").get.outputPath)
                .as(Encoders.bean(classOf[Event[MedicalAct]])))
    assertDSs(new Patients(PatientsConfig(fallConfig.base.studyStart)).extract(sources),
               spark.read.parquet(meta.get("extract_patients").get.outputPath)
                .as(Encoders.bean(classOf[Patient])))
  }
}
