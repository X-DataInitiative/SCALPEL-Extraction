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

// TODO: Aller voir si MCO_FCSTC__EXE_SPE contient plus d'information sur la spécialité que MCO_FBSTC__EXE_SPE
/**
  * Get specialties of the non medical practitioners in the Dcir:
  * If a specialty is available, it extracts the specialty using MCO_FBSTC_ _EXE_SPE and the
  * practitioner identifier from the database
  **/
object McoCeSpecialtyExtractor extends McoCeExtractor[PractitionerClaimSpeciality] {
  override val columnName: String = ColNames.PractitionnerSpecialty
  override val eventBuilder: EventBuilder = MedicalPractitionerClaim

  override def code: Row => String = (row: Row) => row.getAs[Int](columnName).toString


  override def isInStudy(codes: Set[String])
                        (row: Row): Boolean = codes.contains(code(row))

  override def isInExtractorScope(row: Row): Boolean = {
    (!row.isNullAt(row.fieldIndex(columnName))) & (row.getAs[Integer](columnName) != 0)
  }
}
