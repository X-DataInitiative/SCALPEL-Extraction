package fr.polytechnique.cmap.cnam.etl.events
import java.sql.Timestamp

object ProductOrService extends ProductOrService

trait ProductOrService extends AnyEvent with EventBuilder{
  override val category: EventCategory[ProductOrService] = "product_or_service"

  def apply(patientID: String, groupID: String, name: String, quantity: Double, date: Timestamp): Event[ProductOrService] =
    Event(patientID, category, groupID, name, quantity, date, None)
}
