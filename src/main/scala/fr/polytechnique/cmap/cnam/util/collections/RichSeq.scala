package fr.polytechnique.cmap.cnam.util.collections

class RichSeq[A](seq: Seq[A]) {

  def existAll(ps: ((A) => Boolean)*): Boolean = {
    ps.map(seq.exists).reduce(_ && _)
  }
}
