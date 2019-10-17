// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.collections

class RichSeq[A](val seq: Seq[A]) {

  def existAll(ps: ((A) => Boolean)*): Boolean = {
    ps.map(seq.exists).reduce(_ && _)
  }
}
