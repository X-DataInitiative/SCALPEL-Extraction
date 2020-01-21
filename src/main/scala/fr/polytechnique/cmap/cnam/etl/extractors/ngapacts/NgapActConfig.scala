package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

import scala.util.Try
import org.apache.spark.sql.Row

/**
  * NgapActConfig defines three different ways to filter for specific ngap acts in the SNDS :
  * The base configuration is NgapActClassConfig which can filters on :
  *   - prestation type (ngapPrsNatRefs: PRS_NAT_REF),
  *   - prestation coefficient (ngapKeyLetters : PRS_NAT_CB2 or ACT_COD in the PMSI_CE),
  *   - prestation coefficient (ngapCoefficients: PRS_ACT_CFT or ACT_COE in the PMSI_CE)
  * **Note**: If acts_categories is empty, all ngap acts are extracted.
  * The Ngap acts can be found in two sources. The filtering logic differs depending on the source.
  *
  * In the Dcir, search where ngapKeyLetter is available (ie. TODO what proportion in echantillon 2008-2016):
  *   - If a list of ngapPrsNatRefs is given, it extracts all of these PrsNatRef
  *   - if a list of ngapKeyLetters and a list of ngapCoefficients is given, it extracts all combination of (keyLetter, coefficient)
  *
  * In the Pmsi (only McoCe implemented, less than 12000 ngap acts per year in SSR_CE),
  * search where ngapCoefficient is available
  *   - if a list of ngapKeyLetters and a list of ngapCoefficients is given, it extracts all combination of (keyLetter, coefficient)
  *   - if the list of ngapCoefficients is empty, extract all acts where coeff is in ngapCoefficient
  * @param acts_categories List of configuration to get specific NgapActs
  */
class NgapActConfig(
  val acts_categories: List[NgapActClassConfig]) extends ExtractorConfig with Serializable {

  def dcirIsInCategory(
    categories: List[NgapActClassConfig],
    row: Row): Boolean = {

    val ngapKeyLetter : String = row.getAs[String]("PRS_NAT_CB2")
    val ngapCoefficient : String = row.getAs[Double]("PRS_ACT_CFT").toString
    val prsNatRef: String = row.getAs[Int]("PRS_NAT_REF").toString

    categories
      .exists(category =>
        (category.ngapKeyLetters.contains(ngapKeyLetter) &&
          category.ngapCoefficients.contains(ngapCoefficient)) ||
        category.ngapPrsNatRefs.contains(prsNatRef)
      )
  }

  def pmsiIsInCategories(
    categories: List[NgapActClassConfig],
    ngapKeyColumn: String,
    ngapCoeffColumn: String,
    row: Row): Boolean = {

    val letter = row.getAs[String](ngapKeyColumn)
    val coeff = Try(row.getAs[Double](ngapCoeffColumn).toString) recover {
      case _: NullPointerException => "0"
    }

    categories
      .exists(category => pmsiIsInCategory(category, letter, coeff.get))
  }

  def pmsiIsInCategory(
    category: NgapActClassConfig,
    ngapLetter: String,
    ngapCoeff: String): Boolean = {
      if (category.ngapCoefficients.isEmpty) {
        category.ngapKeyLetters.contains(ngapLetter)
      }
      else {
        category.ngapCoefficients.contains(ngapCoeff) &&
          category.ngapKeyLetters.contains(ngapLetter)
      }
  }
}

object NgapActConfig {
  def apply(acts_categories: List[NgapActClassConfig]): NgapActConfig= new NgapActConfig(
    acts_categories
  )

}