// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.productsorservices

import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, EventBuilder, ProductOrService}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.events.acts.DcirRowActExtractor
import org.apache.spark.sql.Row

import scala.util.Try

final case class DcirProductOrServiceExtractor(codes: SimpleExtractorCodes)
  extends DcirRowActExtractor[ProductOrService](codes) {
  override val columnName: String = ColNames.TipCode
  override val eventBuilder: EventBuilder = ProductOrService

  // Implementation of the EventRowExtractor
  override def usedColumns: List[String] = List(
    ColNames.TipCode, ColNames.TipQuantity,
    ColNames.InstitutionCode, ColNames.GHSCode, ColNames.Sector
  ) ++ super.usedColumns

  // Number of products/services
  override def extractWeight(r: Row): Double = r.getAs[Int](ColNames.TipQuantity).toDouble
}
