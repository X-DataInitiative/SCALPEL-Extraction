package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

class NgapActConfig(
  val acts_categories: List[NgapActClassConfig]) extends ExtractorConfig with Serializable {

  def isInCategory(
    categories: List[NgapActClassConfig],
    row: Row): Boolean = {

    val ngapKeyLetter : String = row.getAs[String]("PRS_NAT_CB2")
    val ngapCoefficient : String = row.getAs[Double]("PRS_ACT_CFT").toString

    categories
      .exists(category => category.ngapKeyLetters.contains(ngapKeyLetter) &&
        category.ngapCoefficients.contains(ngapCoefficient)
      )
  }
}

object NgapActConfig {
  def apply(acts_categories: List[NgapActClassConfig]): NgapActConfig= new NgapActConfig(
    acts_categories
  )

}