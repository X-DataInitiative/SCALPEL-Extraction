// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.extractors

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HospitalStay, McoHospitalStay}
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mco.McoSimpleExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.IsInStrategy

class FallHospitalStayExtractor(codes: SimpleExtractorCodes) extends McoSimpleExtractor[HospitalStay]
  with IsInStrategy[HospitalStay] {
  val exitCodes: (String) => (ExitMode) = {
    case "0" => TransferAct
    case "6" => Mutation
    case "7" => Transfer
    case "8" => Home
    case "9" => Death
    case _ => Unknown
  }

  override def getCodes: SimpleExtractorCodes = codes

  override def columnName: String = ColNames.ExitMode

  override def eventBuilder: EventBuilder = McoHospitalStay

  override def neededColumns: List[String] = List(ColNames.EndDate, ColNames.ExitMode) ++ super.usedColumns

  override def extractEnd(r: Row): Option[Timestamp] = Some {
    if (!r.isNullAt(r.fieldIndex(ColNames.EndDate))) {
      new Timestamp(r.getAs[Date](ColNames.EndDate).getTime)
    }
    else { // This shouldn't happen, but some hospital stays come without an EndDate
      extractStart(r)
    }

  }

  override def extractValue(row: Row): String = exitCodes(row.getAs[String](columnName)).value
}

sealed trait ExitMode extends Serializable {
  def value: String
}

object Death extends ExitMode {
  override def value: String = "death"
}

object Mutation extends ExitMode {
  override def value: String = "mutation"
}

object Transfer extends ExitMode {
  override def value: String = "transfer"
}

object Home extends ExitMode {
  override def value: String = "home"
}

object TransferAct extends ExitMode {
  override def value: String = "transfer_act"
}

object Unknown extends ExitMode {
  override def value: String = "unknown"
}
