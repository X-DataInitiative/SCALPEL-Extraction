// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.collections.implicits

import org.scalatest.flatspec.AnyFlatSpec
import fr.polytechnique.cmap.cnam.util.collections.RichSeq

class CollectionsImplicitsSuite extends AnyFlatSpec {

  "this" should "implicitly convert Sequences" in {
    // Given
    val list: List[Int] = List(1, 2, 3, 4)
    // When
    val richSeq: RichSeq[Int] = list
    // Then
    assert(richSeq.seq == list)
  }
}
