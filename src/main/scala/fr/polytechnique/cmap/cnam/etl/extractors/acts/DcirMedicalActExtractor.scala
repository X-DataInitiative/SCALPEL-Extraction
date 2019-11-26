// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.acts

import scala.util.Try
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{BiologyDcirAct, DcirAct, EventBuilder, MedicalAct}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait DcirActExtractor extends DcirExtractor[MedicalAct] {

  private final val PrivateInstitutionCodes = Set(4D, 5D, 6D, 7D)

  override def getInput(sources: Sources): DataFrame = sources.dcir.get.select(
    ColNames.PatientID, ColNames.CamCode, ColNames.BioCode, ColNames.Date,
    ColNames.InstitutionCode, ColNames.GHSCode, ColNames.Sector
  )

  override def extractGroupId(r: Row): String = {
    getGroupId(r) recover { case _: IllegalArgumentException => DcirAct.groupID.DcirAct }
  }.get

  /**
   * Get the information of the origin of DCIR act that is being extracted. It returns a
   * Failure[IllegalArgumentException] if the DCIR schema is old, a success if the DCIR schema contains an information.
   *
   * @param r the row of DCIR to be investigated.
   * @return Try[String]
   */
  def getGroupId(r: Row): Try[String] = Try {

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
  }

  def getGHS(r: Row): Double = r.getAs[Double](ColNames.GHSCode)

  def getInstitutionCode(r: Row): Double = r.getAs[Double](ColNames.InstitutionCode)

  def getSector(r: Row): Double = r.getAs[Double](ColNames.Sector)

  override def extractWeight(r: Row): Double = 1.0

}


object DcirMedicalActExtractor extends DcirActExtractor  {
  override val columnName: String = ColNames.CamCode
  override val eventBuilder: EventBuilder = DcirAct
}


object DcirBiologyActExtractor extends DcirActExtractor  {
  override val columnName: String = ColNames.BioCode
  override val eventBuilder: EventBuilder = BiologyDcirAct
  override def code = (row: Row) => row.getAs[Double](columnName).toString
}