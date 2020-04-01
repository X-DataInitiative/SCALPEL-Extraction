// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.prestations

import java.sql.Timestamp
import scala.util.Try
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.IsInStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.dcir.DcirSimpleExtractor

sealed abstract class DcirPractitionerSpecialityExtractor(codes: SimpleExtractorCodes)
  extends DcirSimpleExtractor[PractitionerClaimSpeciality] with IsInStrategy[PractitionerClaimSpeciality] {

  override def usedColumns: List[ColName] = ColNames.ExecPSNum :: super.usedColumns

  override def extractStart(r: Row): Timestamp = {
    Try(super.extractStart(r)) recover {
      case _: NullPointerException => extractFluxDate(r)
    }
  }.get

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.ExecPSNum)
  }

  override def extractValue(row: Row): String = row.getAs[Integer](columnName).toString

  override def isInExtractorScope(row: Row): Boolean = {
    (!row.isNullAt(row.fieldIndex(columnName))) & (row.getAs[Integer](columnName) != 0)
  }

  override def getCodes: SimpleExtractorCodes = codes
}

/**
  * Get specialties of medical practitioners in the Dcir:
  * If a specialty is available, it extracts the specialty using PSE_SPE_COD and the practitioner
  * identifier from the database.
  */
final case class MedicalPractitionerClaimExtractor(codes: SimpleExtractorCodes)
  extends DcirPractitionerSpecialityExtractor(codes) {
  override val columnName: String = ColNames.MSpe
  override val eventBuilder: EventBuilder = MedicalPractitionerClaim
}


/**
  * Get specialties of the non medical practitioners in the Dcir:
  * If a specialty is available, it extracts the specialty using PSE_ACT_NAT and the practitioner
  * identifier from the database.
  */
final case class NonMedicalPractitionerClaimExtractor(codes: SimpleExtractorCodes)
  extends DcirPractitionerSpecialityExtractor(codes) {
  override val columnName: String = ColNames.NonMSpe
  override val eventBuilder: EventBuilder = NonMedicalPractitionerClaim
}
