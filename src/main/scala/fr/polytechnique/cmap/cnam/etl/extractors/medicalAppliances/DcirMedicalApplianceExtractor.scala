// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.medicalAppliances

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events.{DcirMedicalAppliance, EventBuilder, MedicalAppliance}
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.util.functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Try


/**
  * Extract Medical Appliance delivered by the health care system. Exemples of such prestations are :
  * dietary foods and articles for dressings, external orthoses and prostheses, implantable medical
  * devices and vehicles for the physically handicapped.
  */
trait DcirMedicalApplianceExtractor extends DcirExtractor[MedicalAppliance] {

  override def extractStart(r: Row): Timestamp = {
    Try(super.extractStart(r)) recover {
      case _ => functions.makeTS(1970, 1, 1)
    }
    }.get

  override val columnName: String = ColNames.LppCode
  override val eventBuilder: EventBuilder = DcirMedicalAppliance


  override def getInput(sources: Sources): DataFrame = {
    val inputCols = List(
      ColNames.PatientID, ColNames.LppCode, ColNames.LppPrestationType, ColNames.LppQuantity,
      ColNames.Date, ColNames.LppDisposalEndDate) ::: ColNames.joinKeys

    sources.dcir.get.select(
      inputCols.map(col): _*
    )
  }

  override def extractEnd(r: Row): Option[Timestamp] = {
    Try(Some(r.getAs[java.util.Date](ColNames.LppDisposalEndDate).toTimestamp)) recover {
      case _: NullPointerException => Some(functions.makeTS(1970, 1, 1))
    }
    }.get

  override def extractWeight(r: Row): Double = {
    r.getAs[Int](ColNames.LppQuantity).asInstanceOf[Double]
  }
}


object DcirMedicalApplianceExtractor extends DcirMedicalApplianceExtractor