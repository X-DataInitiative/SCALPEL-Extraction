// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.productsorservices

import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, EventBuilder, ProductOrService}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir.DcirSimpleExtractor
import org.apache.spark.sql.Row

import scala.util.Try

/**
  * Gets all type of Acts from DCIR.
  *
  * The main addition of this class is the groupId method that allows to get the
  * source of the act: Liberal, PublicAmbulatory, PrivateAmbulatory, Unkown and when
  * the information is not available a default DCIRAct.
  *
  * @param codes : List of Act codes to be tracked in the study or empty to get all the Acts.
  */
final case class DcirProductOrServiceExtractor(codes: SimpleExtractorCodes) extends DcirSimpleExtractor[ProductOrService]
  with StartsWithStrategy[ProductOrService] {

  override val columnName: String = ColNames.TipCode
  override val eventBuilder: EventBuilder = ProductOrService

  // Implementation of the Extractor Trait
  override def getCodes: SimpleExtractorCodes = codes

  // Implementation of the EventRowExtractor
  override def usedColumns: List[String] = List(
    ColNames.TipCode, ColNames.TipQuantity,
    ColNames.InstitutionCode, ColNames.GHSCode, ColNames.Sector
  ) ++ super.usedColumns

  // Number of products/services
  override def extractWeight(r: Row): Double = r.getAs[Int](ColNames.TipQuantity).toDouble

  final val PrivateInstitutionCodes = Set(4D, 5D, 6D, 7D)

  /**
    * Get the information of the origin of DCIR act that is being extracted. It returns a
    * Failure[IllegalArgumentException] if the DCIR schema is old, a success if the DCIR schema contains an information.
    *
    * @param r the row of DCIR to be investigated.
    * @return Try[String]
    */
  // TODO: REMOVE THIS
  override def extractGroupId(r: Row): String = {
    Try {

      if (!r.isNullAt(r.fieldIndex(ColNames.Sector)) && getSector(r) == 1) {
        DcirAct.groupID.PublicAmbulatory
      }
      else {
        if (r.isNullAt(r.fieldIndex(ColNames.GHSCode))) {
          DcirAct.groupID.Liberal
        } else {
          // Value is not at null, it is not liberal
          lazy val ghs = getGHS(r)
          lazy val institutionCode = getInstitutionCode(r)
          // Check if it is a private ambulatory
          if (ghs == 0 && PrivateInstitutionCodes.contains(institutionCode)) {
            DcirAct.groupID.PrivateAmbulatory
          }
          else {
            DcirAct.groupID.Unknown
          }
        }
      }
    } recover { case _: IllegalArgumentException => DcirAct.groupID.DcirAct }
  }.get

  private def getGHS(r: Row): Double = r.getAs[Double](ColNames.GHSCode)

  private def getInstitutionCode(r: Row): Double = r.getAs[Double](ColNames.InstitutionCode)

  private def getSector(r: Row): Double = r.getAs[Double](ColNames.Sector)
}