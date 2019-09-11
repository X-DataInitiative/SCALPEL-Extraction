package fr.polytechnique.cmap.cnam

package object exceptions {

  case class WrongMatchException(message: String) extends Exception

}
