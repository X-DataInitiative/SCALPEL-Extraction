package fr.polytechnique.cmap.cnam.etl.events

trait AnyEvent {
  val category: EventCategory[AnyEvent]
}
