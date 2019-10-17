// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.collections

package object implicits {

  implicit def toRichSeq[A](value: Seq[A]): RichSeq[A] = new RichSeq[A](value)
}
