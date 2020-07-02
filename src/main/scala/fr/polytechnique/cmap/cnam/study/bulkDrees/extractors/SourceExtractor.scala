// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulkDrees.extractors

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.extractors.codes.ExtractorCodes
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.reporting.{OperationMetadata, OperationReporter, OperationTypes}
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/**
  * Extract all available Events from the given source.
  *
  * This regroups all the available Extractors for a given source and execute them on the Source. If the running passes,
  * then the result is stored in the given path and saved in the OperationMetadata. If the running fails, then the
  * logger warns the user that the running failed, indicating the missing tables that must flattened.
  *
  * Every implementation of this abstract class must updated whenever a new Extractor that works on the given Source is
  * added.
  */
abstract class SourceExtractor(val path: String, val saveMode: String) {
  val sourceName: String

  // This the ugliest bit of this implementation, and there is no way getting around it because of Spark.
  // First, Spark Dataset is invariant hence no way of making the Extractor trait covariant to avoid this ugly upper
  // bounding.
  // Second, TypeTag is needed for the Spark encoder for case class, hence the explicit typing instead of AnyEvent.
  // @TODO: Every time you add a new Event type you will need to add it in the "with" clause
  val extractors: List[ExtractorSources[_ >: MedicalAct with HospitalStay with Diagnosis with Drug
    with MedicalTakeOverReason with NgapAct with PractitionerClaimSpeciality <: AnyEvent with EventBuilder,
    ExtractorCodes]]
  private val logger = Logger.getLogger(this.getClass)

  /**
    * Extract all Events from the Source and returns a List of OperationMetadata.
    *
    * @param sources Sources object containing the sources.
    * @return OperationMetadata containing all Events extracted.
    */
  def extract(sources: Sources): List[OperationMetadata] = extractors.flatMap(es => runAndReport(sources)(es))

  def runAndReport[A <: AnyEvent : TypeTag](sources: Sources)(es: ExtractorSources[A, ExtractorCodes]): Option[OperationMetadata] =
    run(es.extractor, sources) match {
      case Success(tde) => Some(report(es, tde))
      case Failure(error) => {
        logger.warn(
          "Extractor " + es
            .extractor + " failed, probably you didn't flatten all the following tables" + es.sources
        )
        None
      }
    }

  def run[A <: AnyEvent : TypeTag](extractor: Extractor[A, ExtractorCodes], sources: Sources): Try[Dataset[Event[A]]] = {
    Try {
      extractor.extract(sources)(typeTag[A])
    }
  }

  def report[A <: AnyEvent : TypeTag](
    extractorSources: ExtractorSources[A, ExtractorCodes],
    result: Dataset[Event[A]]): OperationMetadata = OperationReporter
    .report(
      extractorSources.name,
      extractorSources.sources,
      OperationTypes.AnyEvents,
      result.toDF,
      Path(path),
      saveMode
    )
}


case class ExtractorSources[EventType <: AnyEvent : TypeTag, +Codes <: ExtractorCodes](
  extractor: Extractor[EventType, Codes],
  sources: List[String],
  name: String)