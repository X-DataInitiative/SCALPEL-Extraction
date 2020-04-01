// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.acts

import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{BiologyDcirAct, DcirAct, EventBuilder, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.StartsWithStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir.DcirSimpleExtractor
import fr.polytechnique.cmap.cnam.util.functions.makeTS

abstract sealed class DcirRowActExtractor(codes: SimpleExtractorCodes) extends DcirSimpleExtractor[MedicalAct]
  with StartsWithStrategy[MedicalAct] {

  final val PrivateInstitutionCodes = Set(4D, 5D, 6D, 7D)

  // Implementation of the Extractor Trait
  override def getCodes: SimpleExtractorCodes = codes

  // Implementation of the EventRowExtractor
  override def usedColumns: List[String] = List(ColNames.InstitutionCode, ColNames.GHSCode, ColNames.Sector) ++ super
    .usedColumns

  override def extractStart(r: Row): Timestamp = {
    Try(super.extractStart(r)) recover {
      case _ => makeTS(1970, 1, 1)
    }
  }.get

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

final case class DcirMedicalActExtractor(codes: SimpleExtractorCodes)
  extends DcirRowActExtractor(codes) {
  // Implementation of the BasicExtractor Trait
  override val columnName: String = ColNames.CamCode
  override val eventBuilder: EventBuilder = DcirAct
}

final case class DcirBiologyActExtractor(codes: SimpleExtractorCodes)
  extends DcirRowActExtractor(codes) {
  // Implementation of the BasicExtractor Trait
  override val columnName: String = ColNames.BioCode
  override val eventBuilder: EventBuilder = BiologyDcirAct

  // Because BioCode is a Double
  override def extractValue(row: Row): String = row.getAs[Double](columnName).toString

}
