// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.observation

import java.sql.Timestamp

case class ObservationPeriod(patientID: String, start: Timestamp, stop: Timestamp)
