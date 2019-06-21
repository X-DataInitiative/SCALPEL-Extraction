package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp
import org.apache.spark.sql._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.datetime.implicits._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import scala.util.Try

class DCIRMedicalActEventExtractor(codes: Seq[String]) extends Serializable with DCIRSourceInfo {
  def extract(r: Row): List[Event[MedicalAct]] = {
    val idx = r.fieldIndex(PatientCols.CamCode)
    codes.find(!r.isNullAt(idx) && r.getString(idx).startsWith(_))
      .map {
        code =>
        val groupID = {
          getGroupId(r) recover { case _: IllegalArgumentException => DcirAct.groupID.DcirAct }
        }.get
        DcirAct(
          patientID = r.getAs[String](PatientCols.PatientID),
          groupID = groupID,
          code = code,
          date = r.getAs[java.util.Date](PatientCols.Date).toTimestamp
        )
      }.toList
  }

  def getGroupId(r: Row): Try[String] = Try {
    val sector = r.getAs[Double](PatientCols.Sector)
    if (!r.isNullAt(r.fieldIndex(PatientCols.Sector)) && sector != 2) {
      DcirAct.groupID.PublicAmbulatory
    }
    else {
      if (r.isNullAt(r.fieldIndex(PatientCols.GHSCode))) {
        DcirAct.groupID.Liberal
      } else {
        // Value is not at null, it is not liberal
        lazy val ghs = r.getAs[Double](PatientCols.GHSCode)
        lazy val institutionCode = r.getAs[Double](PatientCols.InstitutionCode)
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
}

class DCIRSourceExtractor(val sources : Sources, val extractors: List[DCIRMedicalActEventExtractor]) extends SourceExtractor with DCIRSourceInfo {

  override def select() : DataFrame = {
    sources.dcir.get
  }

  def extract[A <: AnyEvent](): Dataset[Event[A]] = {
    val df = select()
    import df.sqlContext.implicits._
    df.flatMap {
      r : Row =>
      extractors.flatMap(extractor => extractor.extract(r)) }
                .asInstanceOf[Dataset[Event[A]]]
  }
}

