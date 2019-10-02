package fr.polytechnique.cmap.cnam.etl.extractors.prestations

import java.sql.Timestamp

import scala.util.Try
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.dcir.DcirExtractor

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
}

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

  override def isInStudy(codes: Set[String])
    (row: Row): Boolean = codes.contains(code(row))
}


// TODO add MCO_ce pfs num + SSR_CE ce pfs_num