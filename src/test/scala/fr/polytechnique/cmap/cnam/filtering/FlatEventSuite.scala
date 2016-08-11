package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.SharedContext


class FlatEventSuite extends SharedContext {

    "convert" should "fuse one Event and one Patient with the same patientID" in {
      // Given
      val event = Event("bob", "theSponge", "412", 42.00, Timestamp.valueOf("2001-01-01 00:00:00"), None)
      val patient = Patient("bob", 12, Timestamp.valueOf("1901-01-01 00:00:00"), Some(Timestamp.valueOf("2001-01-01 00:00:00")))

      val expected = FlatEvent("bob", 12, Timestamp.valueOf("1901-01-01 00:00:00"), Some(Timestamp.valueOf("2001-01-01 00:00:00")),
        "theSponge", "412", 42.00, Timestamp.valueOf("2001-01-01 00:00:00"), None)

      // When
      val result = FlatEvent.merge(event, patient)

      // Then
      assert(result == expected)
    }


}
