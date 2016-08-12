package fr.polytechnique.cmap.cnam

/**
  * Created by burq on 11/08/16.
  */
object exceptions {
  case class WrongMatchException(message: String) extends Exception
}
