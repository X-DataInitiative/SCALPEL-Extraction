// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk.extractors


import scala.reflect.runtime.universe
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, McoCIM10Act, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import fr.polytechnique.cmap.cnam.util.reporting.{OperationMetadata, OperationTypes}
import fr.polytechnique.cmap.cnam.util.Path

class SourceExtractorSuite extends SharedContext {
  lazy val sqlCtx: SQLContext = super.sqlContext

  // This shouldn't be replicated anywhere and Mocking should be the preferred technique.
  // Mocking the Extractor is not possible because of type erasure. Type erasure make the typing of the implicit
  // in the extractor method of trait Extractor as the type is not known at compile time, but only at run time.
  val  testExtractor = new Extractor[MedicalAct] with Serializable {
    override def isInStudy(codes: Set[String])
      (row: Row): Boolean = true

    override def isInExtractorScope(row: Row): Boolean = true

    override def builder(row: Row): Seq[Event[MedicalAct]] =
      Seq(McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 31)))

    override def getInput(sources: Sources): DataFrame = {
      import sqlCtx.implicits._
      Seq[Event[MedicalAct]](
        McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 31)),
        McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 31)),
        McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 10))
      ).toDF
    }

    override def extract(
      sources: Sources,
      codes: Set[String])
      (implicit ctag: universe.TypeTag[MedicalAct]): Dataset[Event[MedicalAct]] = {
      import sqlCtx.implicits._
      Seq[Event[MedicalAct]](
        McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 31)),
        McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 31)),
        McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 10))
      ).toDS
    }
  }

  "extract" should "produce run and report the Extractors" in {
    import sqlCtx.implicits._

    // Given

    val sources = Sources()
    val ds = Seq[Event[MedicalAct]](
      McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 31)),
      McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 31)),
      McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 10))
    ).toDS
    val path = "target/test/output"

    val expected = List(
      OperationMetadata(
        "Mock",
        List("Mock"),
        OperationTypes.AnyEvents,
        Path(path, "Mock", "data").toString,
        Path(path, "Mock", "patients").toString
      )
    )
    // When
    val se: SourceExtractor = new SourceExtractor(path, "overwrite") {
      override val sourceName: String = "Test"
      override val extractors: List[ExtractorSources[MedicalAct]] =
        List(ExtractorSources[MedicalAct](testExtractor, List("Mock"), "Mock"))
    }

    val result = se.extract(sources)
    // Then
    assert(result == expected)
  }
}
