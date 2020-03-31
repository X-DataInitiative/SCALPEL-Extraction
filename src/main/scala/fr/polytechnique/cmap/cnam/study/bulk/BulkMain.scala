// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.bulk

import java.io.PrintWriter
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.extractors.acts.{DcirMedicalActExtractor, McoCcamActExtractor, McoCeCcamActExtractor}
import fr.polytechnique.cmap.cnam.etl.extractors.classifications.GhmExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.diagnoses._
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.DrugExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.patients.{Patients, PatientsConfig}
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.bulk.extractors._
import fr.polytechnique.cmap.cnam.util.reporting.MainMetadata

/*object BulkMain extends Main {
  override def appName: String = "BulkMain"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    val bulkConfig = BulkConfig.load(argsMap("conf"), argsMap("env"))

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(bulkConfig.input))

    val sourceExtractor: List[SourceExtractor] = List(
      new DcirSourceExtractor(bulkConfig.output.root, bulkConfig.output.saveMode, bulkConfig.drugs),
      new McoSourceExtractor(bulkConfig.output.root, bulkConfig.output.saveMode),
      new McoCeSourceExtractor(bulkConfig.output.root, bulkConfig.output.saveMode),
      new SsrSourceExtractor(bulkConfig.output.root, bulkConfig.output.saveMode),
      new SsrCeSourceExtractor(bulkConfig.output.root, bulkConfig.output.saveMode),
      new HadSourceExtractor(bulkConfig.output.root, bulkConfig.output.saveMode)
    )

    // Write Metadata
    val metadata = MainMetadata(
      this.getClass.getName, startTimestamp, new java.util.Date(),
      sourceExtractor.map(se => se.extract(sources)).flatten ++
        new PatientExtractor(bulkConfig.output.root, bulkConfig.output.saveMode, bulkConfig.base).extract(sources)
    )
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_bulk_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }

    None
  }
}*/
