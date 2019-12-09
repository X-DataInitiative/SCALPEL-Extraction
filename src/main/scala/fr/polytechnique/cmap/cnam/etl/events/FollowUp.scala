// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row


object FollowUp extends FollowUp


trait FollowUp extends AnyEvent with EventBuilder {

  val category: EventCategory[FollowUp] = "follow_up"

  def fromRow(
    r: Row,
    patientIDCol: String = "patientID",
    endReason: String = "endReason",
    startCol: String = "start",
    endCol: String = "end"): Event[FollowUp] = {

    FollowUp(
      r.getAs[String](patientIDCol),
      r.getAs[String](endReason),
      r.getAs[Timestamp](startCol),
      r.getAs[Timestamp](endCol)
    )
  }


  def apply(patientID: String, endReason: String, start: Timestamp, end: Timestamp): Event[FollowUp] =
    Event(patientID, category, groupID = "NA", endReason, weight = 0D, start, Some(end))
}

