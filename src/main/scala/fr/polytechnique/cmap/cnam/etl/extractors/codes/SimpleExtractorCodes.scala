// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.codes

import scala.collection.immutable.HashSet

class SimpleExtractorCodes(val codes: List[String]) extends ExtractorCodes {
  val internalCodes: HashSet[String] = codes.to[HashSet]

  override def isEmpty: Boolean = internalCodes.isEmpty

  def exists(p: String => Boolean): Boolean = internalCodes.exists(p)

  def contains(code: String): Boolean = internalCodes.contains(code)
}

object SimpleExtractorCodes {
  def empty = new SimpleExtractorCodes(List.empty)

  def apply(codes: List[String]): SimpleExtractorCodes = new SimpleExtractorCodes(codes)
}