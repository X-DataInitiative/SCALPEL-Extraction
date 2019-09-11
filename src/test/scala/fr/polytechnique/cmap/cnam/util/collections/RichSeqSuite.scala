package fr.polytechnique.cmap.cnam.util.collections

import org.scalatest.FlatSpec

class RichSeqSuite extends FlatSpec {

  "existAll" should "return true if all given predicates \"exist\" in the collection" in {

    // Given
    val input: RichSeq[Int] = new RichSeq(List(1, 2, 3, 4, 5))
    val predicate1: (Int) => Boolean = (i: Int) => {
      i % 5 == 0
    }
    val predicate2: (Int) => Boolean = (i: Int) => {
      i == 0 || i == 3
    }

    // When
    val trueResult = input.existAll(predicate1, predicate2)

    // Then
    assert(trueResult)
  }

  it should "return false if at least one predicate does not \"exist\" in the collection" in {

    // Given
    val input: RichSeq[Int] = new RichSeq(List(1, 2, 3, 4, 5))
    val truePredicate: (Int) => Boolean = (i: Int) => {
      i % 5 == 0 || i % 2 == 0
    }
    val falsePredicate: (Int) => Boolean = (i: Int) => {
      i <= 0 || i > 5
    }
    // When
    val falseResult = input.existAll(truePredicate, falsePredicate)

    // Then
    assert(!falseResult)
  }
}
