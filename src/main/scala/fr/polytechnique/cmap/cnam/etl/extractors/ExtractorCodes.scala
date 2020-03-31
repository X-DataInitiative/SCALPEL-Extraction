// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import scala.collection.immutable.HashSet

trait ExtractorCodes extends Serializable {
  def isEmpty: Boolean
}

class BaseExtractorCodes(val codes: List[String]) extends ExtractorCodes {
  val internalCodes: HashSet[String] = codes.to[HashSet]

  override def isEmpty: Boolean = internalCodes.isEmpty

  def exists(p: String => Boolean): Boolean = internalCodes.exists(p)

  def contains(code: String): Boolean = internalCodes.contains(code)
}

object BaseExtractorCodes {
  def empty = new BaseExtractorCodes(List.empty)

  def apply(codes: List[String]): BaseExtractorCodes = new BaseExtractorCodes(codes)

}
