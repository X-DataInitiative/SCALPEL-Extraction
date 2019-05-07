package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

class NgapActConfig(
  val acts_categories: List[NgapActClassConfig]) extends ExtractorConfig with Serializable {

  def isInCategory(
                    categories: List[NgapActClassConfig],
                    row: Row): Boolean = categories
      .exists(category => category.naturePrestation == row.getAs[Int]("PRS_NAT_REF") &&
        category.ngapCoefficients.contains(row.getAs[Double]("PRS_ACT_CFT").toString)
      )
}

object NgapActConfig {
  def apply(acts_categories: List[NgapActClassConfig]): NgapActConfig= new NgapActConfig(
    acts_categories
  )

}