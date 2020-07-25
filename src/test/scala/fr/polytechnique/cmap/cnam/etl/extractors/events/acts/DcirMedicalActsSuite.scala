// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir.DcirSource

class DcirMedicalActsSuite extends SharedContext {

  val colNames = new DcirSource {}.ColNames

  val schema = StructType(
    StructField(colNames.PatientID, StringType) ::
      StructField(colNames.CamCode, StringType) ::
      StructField(colNames.InstitutionCode, DoubleType) ::
      StructField(colNames.GHSCode, DoubleType) ::
      StructField(colNames.Sector, DoubleType) ::
      StructField(colNames.FlowDistributionDate, DateType) :: Nil
  )

  val oldSchema = StructType(
    StructField(colNames.PatientID, StringType) ::
      StructField(colNames.CamCode, StringType) ::
      StructField(colNames.FlowDistributionDate, DateType) :: Nil
  )

  /*  "extract" should "return a Dataset of DCIR Medical Acts" in {

      val sqlCtx = sqlContext
      import sqlCtx.implicits._

      // Given
      val codes = BaseExtractorCodes(List("AAAA", "CCCC", "DDDD"))

      val input = Seq(
        ("Patient_A", "AAAA", "NABM1", makeTS(2010, 1, 1), None, None, None),
        ("Patient_A", "BBBB", "NABM1", makeTS(2010, 2, 1), Some(1D), Some(0D), Some(1D)),
        ("Patient_B", "CCCC", "NABM1", makeTS(2010, 3, 1), None, None, None),
        ("Patient_B", "CCCC", "NABM1", makeTS(2010, 4, 1), Some(7D), Some(0D), Some(2D)),
        ("Patient_C", "BBBB", "NABM1", makeTS(2010, 5, 1), Some(1D), Some(0D), Some(2D)),
        ("Patient_D", "DDDD", "NABM1", null, None, None, None)
      ).toDF(
        colNames.PatientID, colNames.CamCode, colNames.BioCode, colNames.FlowDistributionDate,
        colNames.InstitutionCode, colNames.GHSCode, colNames.Sector
      )

      val sources = Sources(dcir = Some(input))

      val expected = Seq[Event[MedicalAct]](
        DcirAct("Patient_A", DcirAct.groupID.Liberal, "AAAA", 1.0, makeTS(2010, 1, 1)),
        DcirAct("Patient_B", DcirAct.groupID.Liberal, "CCCC", 1.0, makeTS(2010, 3, 1)),
        DcirAct("Patient_B", DcirAct.groupID.PrivateAmbulatory, "CCCC", 1.0, makeTS(2010, 4, 1)),
        DcirAct("Patient_D", DcirAct.groupID.Liberal, "DDDD", 1.0, makeTS(1970, 1, 1))
      ).toDS

      // When
      val result = DcirMedicalActExtractor(codes).extract(sources)

      // Then
      assertDSs(result, expected)
    }*/
}
