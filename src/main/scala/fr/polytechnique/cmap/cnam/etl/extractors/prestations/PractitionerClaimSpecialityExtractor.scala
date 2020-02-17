// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeExtractor
import org.apache.spark.sql.Row

import scala.util.Try

/**
  * Get specialties of medical practitionner in the Dcir:
  * If a specialty is available, it extracts the specialty using PSE_SPE_COD and the practitioner
  * identifier from the database.
  */
object MedicalPractitionerClaimExtractor extends DcirExtractor[PractitionerClaimSpeciality] {
  override val columnName: String = ColNames.MSpe
  override val eventBuilder: EventBuilder = MedicalPractitionerClaim

  override def code: Row => String = (row: Row) => row.getAs[Integer](columnName).toString

  override def extractStart(r: Row): Timestamp = {
    Try(super.extractStart(r)) recover {
      case _: NullPointerException => extractFluxDate(r)
    }
  }.get

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.ExecPSNum)
  }

  override def isInStudy(codes: Set[String])
                        (row: Row): Boolean = codes.contains(code(row))

  override def isInExtractorScope(row: Row): Boolean = {
    (!row.isNullAt(row.fieldIndex(columnName))) & (row.getAs[Integer](columnName) != 0)
  }
}

/**
  * Get specialties of the non medical practitionners in the Dcir:
  * If a specialty is available, it extracts the specialty using PSE_ACT_NAT and the practitioner
  * identifier from the database.
  */
object NonMedicalPractitionerClaimExtractor extends DcirExtractor[PractitionerClaimSpeciality] {
  override val columnName: String = ColNames.NonMSpe
  override val eventBuilder: EventBuilder = NonMedicalPractitionerClaim

  override def code: Row => String = (row: Row) => row.getAs[Integer](columnName).toString

  override def extractStart(r: Row): Timestamp = {
    Try(super.extractStart(r)) recover {
      case _: NullPointerException => extractFluxDate(r)
    }
  }.get

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.ExecPSNum)
  }

  override def isInExtractorScope(row: Row): Boolean = {
    (!row.isNullAt(row.fieldIndex(columnName))) & (row.getAs[Integer](columnName) != 0)
  }

  override def isInStudy(codes: Set[String])
                        (row: Row): Boolean = codes.contains(code(row))
}


/**
  * Get specialties of the non medical practitioners in the MCO_CE:
  * If a specialty is available, it extracts the specialty using MCO_FBSTC_ _EXE_SPE and MCO_FCSTC_ _EXE_SPE.
  * These two columns are complementary as described here :
  * https://documentation-snds.health-data-hub.fr/fiches/actes_consult_externes.html#les-tables-du-pmsi-version-snds-pour-les-ace
  **/
trait McoCeSpecialtyExtractor extends McoCeExtractor[PractitionerClaimSpeciality] {
  override val eventBuilder: EventBuilder = MedicalPractitionerClaim

  override def code: Row => String = (row: Row) => row.getAs[Int](columnName).toString


  override def isInStudy(codes: Set[String])
                        (row: Row): Boolean = codes.contains(code(row))

  override def isInExtractorScope(row: Row): Boolean = {
    (!row.isNullAt(row.fieldIndex(columnName))) & (row.getAs[Integer](columnName) != 0)
  }
}

object McoCeFbstcSpecialtyExtractor extends McoCeSpecialtyExtractor {
  override val columnName: String = ColNames.PractitionnerSpecialtyFbstc
  override val eventBuilder: EventBuilder = McoCeFbstcMedicalPractitionerClaim
}


object McoCeFcstcSpecialtyExtractor extends McoCeSpecialtyExtractor {
  override val columnName: String = ColNames.PractitionnerSpecialtyFcstc
  override val eventBuilder: EventBuilder = McoCeFcstcMedicalPractitionerClaim
}
