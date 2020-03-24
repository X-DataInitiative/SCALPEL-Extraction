package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import scala.reflect.runtime.universe._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events.{DcirNgapAct, Event, EventBuilder, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DcirNgapActExtractor(ngapActsConfig: NgapActConfig) extends DcirExtractor[NgapAct] {

  private final val PrivateInstitutionCodes = Set(4D, 5D, 6D, 7D)

  override val columnName: String = ColNames.NaturePrestation
  override val eventBuilder: EventBuilder = DcirNgapAct
  val ngapKeyLetter: String = "PRS_NAT_CB2"

  /**
    * We extract Ngap acts as a concatenation of three different ways to identify specific ngap acts in the SNDS :
    *   - prestation type (ngapPrsNatRefs: PRS_NAT_REF),
    *   - prestation coefficient (ngapKeyLetters : PRS_NAT_CB2 or ACT_COD in the PMSI_CE),
    *   - prestation coefficient (ngapCoefficients: PRS_ACT_CFT or ACT_COE in the PMSI_CE)
    *
    * For more information, Cf NgapActConfig documentation.
    *
    * @return concatenation of the three codes
    */
  override def code: Row => String = (row: Row) => {
    row.getAs[Int](ColNames.NaturePrestation).toString + "_" +
      row.getAs[String](ngapKeyLetter) + "_" +
      row.getAs[Double](ColNames.NgapCoefficient).toString
  }

  override def extractGroupId(r: Row): String = {

    if (!r.isNullAt(r.fieldIndex(ColNames.Sector)) && getSector(r) == 1) {
      DcirNgapAct.groupID.PublicAmbulatory
    }
    else {
      if (r.isNullAt(r.fieldIndex(ColNames.GHSCode))) {
        DcirNgapAct.groupID.Liberal
      } else {
        // Value is not at null, it is not liberal
        lazy val ghs = getGHS(r)
        lazy val institutionCode = getInstitutionCode(r)
        // Check if it is a private ambulatory
        if (ghs == 0 && PrivateInstitutionCodes.contains(institutionCode)) {
          DcirNgapAct.groupID.PrivateAmbulatory
        }
        else {
          DcirNgapAct.groupID.Unknown
        }
      }
    }
  }

  def getGHS(r: Row): Double = r.getAs[Double](ColNames.GHSCode)

  def getInstitutionCode(r: Row): Double = r.getAs[Double](ColNames.InstitutionCode)

  def getSector(r: Row): Double = r.getAs[Double](ColNames.Sector)

  override def extractWeight(r: Row): Double = 1.0

  override def extract(
    sources: Sources,
    codes: Set[String])
    (implicit ctag: TypeTag[NgapAct]): Dataset[Event[NgapAct]] = {

    val input: DataFrame = getInput(sources)

    import input.sqlContext.implicits._

    {
      if (ngapActsConfig.actsCategories.isEmpty) {
        input.filter(isInExtractorScope _)
      }
      else {
        input.filter(isInExtractorScope _).filter(isInStudy(codes) _)
      }
    }.flatMap(builder _).distinct()
  }

  override def getInput(sources: Sources): DataFrame = {

    val neededColumns: List[Column] = List(
      ColNames.PatientID, ColNames.NaturePrestation, ColNames.NgapCoefficient,
      ColNames.Date, ColNames.ExecPSNum, ColNames.DcirFluxDate, ngapKeyLetter,
      ColNames.Sector, ColNames.GHSCode, ColNames.InstitutionCode
    ).map(colName => col(colName))

    lazy val irNat = sources.irNat.get
    lazy val dcir = sources.dcir.get

    lazy val df: DataFrame = dcir.join(irNat, dcir("PRS_NAT_REF").cast("String") === irNat("PRS_NAT"))
    df.select(neededColumns: _*)
  }

  override def isInExtractorScope(row: Row): Boolean = {
    !row.isNullAt(row.fieldIndex(ngapKeyLetter))
  }

  override def isInStudy(codes: Set[String])(row: Row): Boolean = {
    dcirIsInCategory(
      ngapActsConfig.actsCategories,
      row
    )
  }

  def dcirIsInCategory(
    categories: List[NgapActClassConfig],
    row: Row): Boolean = {

    val ngapKeyLetter: String = row.getAs[String]("PRS_NAT_CB2")
    val ngapCoefficient: String = row.getAs[Double]("PRS_ACT_CFT").toString
    val prsNatRef: String = row.getAs[Int]("PRS_NAT_REF").toString

    categories
      .exists(
        category =>
          (
            category.ngapKeyLetters.contains(ngapKeyLetter) &&
              category.ngapCoefficients.contains(ngapCoefficient)
            ) ||
            category.ngapPrsNatRefs.contains(prsNatRef)
      )
  }
}