package fr.polytechnique.cmap.cnam.filtering.events

trait AnyEvent {
  val category: EventCategory[AnyEvent]
}
