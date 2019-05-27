package fr.polytechnique.cmap.cnam.etl.events

trait AnyEvent extends Serializable {
  val category: EventCategory[AnyEvent]
}
