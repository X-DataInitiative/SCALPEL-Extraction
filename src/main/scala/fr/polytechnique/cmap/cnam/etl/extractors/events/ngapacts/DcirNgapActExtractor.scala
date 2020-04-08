// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts


import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.{DcirNgapAct, Event, EventBuilder, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir.DcirRowExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

final case class DcirNgapActExtractor(ngapActsConfig: NgapActConfig[NgapWithNatClassConfig])
  extends Extractor[NgapAct, NgapActConfig[NgapWithNatClassConfig]] with DcirRowExtractor {

  val columnName: String = ColNames.NaturePrestation
  val eventBuilder: EventBuilder = DcirNgapAct
  val ngapKeyLetterCol: String = "PRS_NAT_CB2"

  final val PrivateInstitutionCodes = Set(4D, 5D, 6D, 7D)

  override def getInput(sources: Sources): DataFrame = {

    val neededColumns: List[Column] = List(
      ColNames.PatientID, ColNames.NaturePrestation, ColNames.NgapCoefficient,
      ColNames.DcirEventStart, ColNames.ExecPSNum, ColNames.DcirFluxDate, ngapKeyLetterCol,
      ColNames.Sector, ColNames.GHSCode, ColNames.InstitutionCode
    ).map(col)

    lazy val irNat = sources.irNat.get
    lazy val dcir = sources.dcir.get

    lazy val df: DataFrame = dcir.join(irNat, dcir(ColNames.NaturePrestation).cast("String") === irNat("PRS_NAT"))
    df.select(neededColumns: _*)
  }

  override def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(ngapKeyLetterCol))

  override def isInStudy(row: Row): Boolean = {

    lazy val prsNatRef = row.getAs[Int](ColNames.NaturePrestation).toString
    lazy val ngapKeyLetter = row.getAs[String](ngapKeyLetterCol)
    lazy val ngapCoefficient = row.getAs[Double](ColNames.NgapCoefficient).toString

    ngapActsConfig.actsCategories
      .exists(
        category => {
          category.ngapPrsNatRefs.contains(prsNatRef) || {
            category.ngapKeyLetters.contains(ngapKeyLetter) && category.ngapCoefficients.contains(ngapCoefficient)
          }
        }
      )
  }

  def builder(row: Row): Seq[Event[NgapAct]] = {
    val patientId = extractPatientId(row)
    val groupId = extractGroupId(row)
    val value = extractValue(row)
    val eventDate = extractStart(row)
    val endDate = extractEnd(row)
    val weight = extractWeight(row)

    Seq(eventBuilder[NgapAct](patientId, groupId, value, weight, eventDate, endDate))
  }

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
  override def extractValue(row: Row): String = {
    s"${row.getAs[Int](ColNames.NaturePrestation)}_${row.getAs[String](ngapKeyLetterCol)}_${
      row.getAs[Double](ColNames.NgapCoefficient).toString
    }"
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

  private def getGHS(r: Row): Double = r.getAs[Double](ColNames.GHSCode)

  private def getInstitutionCode(r: Row): Double = r.getAs[Double](ColNames.InstitutionCode)

  private def getSector(r: Row): Double = r.getAs[Double](ColNames.Sector)

  override def getCodes: NgapActConfig[NgapWithNatClassConfig] = ngapActsConfig
}