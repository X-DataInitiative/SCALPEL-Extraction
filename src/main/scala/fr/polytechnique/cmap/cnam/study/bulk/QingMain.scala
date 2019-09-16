package fr.polytechnique.cmap.cnam.study.bulk

import java.io.PrintWriter
import scala.collection.mutable
import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.bulk.extractors.{DiabetesDiagnosisExtractor, DiabetesDrugsExtractor, HTAExtractor}
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.reporting.{MainMetadata, OperationMetadata, OperationReporter, OperationTypes}

object QingMain extends Main {
  override def appName: String = "Extraction for Qing"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {
    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()
    val bulkConfig = BulkConfig.load(argsMap("conf"), argsMap("env"))

    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(bulkConfig.input))

    val operationsMetadata = mutable.Buffer[OperationMetadata]()

    val htas = HTAExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report(
        "HTADrugPurchases",
        List("DCIR"),
        OperationTypes.Dispensations,
        htas.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    htas.unpersist()

    val diabetesDrugs = DiabetesDrugsExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report(
        "DiabetesDrugPurchases",
        List("DCIR"),
        OperationTypes.Dispensations,
        diabetesDrugs.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    diabetesDrugs.unpersist()

    val diabetesDiagDiagnoses = DiabetesDiagnosisExtractor.extract(sources).cache()
    operationsMetadata += {
      OperationReporter.report(
        "DiabetesDiagnoses",
        List("MCO"),
        OperationTypes.Diagnosis,
        diabetesDiagDiagnoses.toDF,
        Path(bulkConfig.output.outputSavePath),
        bulkConfig.output.saveMode
      )
    }
    diabetesDiagDiagnoses.unpersist()


    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_extraction_for_Qing_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }

    None
  }
}
