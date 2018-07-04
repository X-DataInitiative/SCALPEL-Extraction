package fr.polytechnique.cmap.cnam.etl.config

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import scala.reflect.ClassTag
import com.typesafe.config.ConfigFactory
import me.danielpes.spark.datetime.Period
import me.danielpes.spark.datetime.implicits._
import pureconfig._
import pureconfig.configurable.{localDateConfigConvert, localDateTimeConfigConvert}

trait ConfigLoader {

  // For reading yyyy-MM-dd dates
  implicit val localDate: ConfigConvert[LocalDate] = {
    localDateConfigConvert(DateTimeFormatter.ISO_DATE)
  }
  implicit val localDateTime: ConfigConvert[LocalDateTime] = {
    localDateTimeConfigConvert(DateTimeFormatter.ISO_DATE_TIME)
  }

  // For reading snake_case config items
  implicit def productHint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, SnakeCase))

  // For reading enums.
  implicit def coproductHint[T]: CoproductHint[T] = new EnumCoproductHint[T] {
    // The following override allows converting from snake_case (in the config file) to CamelCase.
    // I found this by looking at pureconfig's code, not the docs.
    override def fieldValue(name: String): String = SnakeCase.fromTokens(CamelCase.toTokens(name))
  }

  // For reading Periods (check pureconfig's docs about "Supporting new Types")
  implicit val myPeriodReader: ConfigReader[Period] = ConfigReader[String].map { s =>
    val (n, unit) = (s.split(" ")(0).toInt, s.split(" ")(1))
    unit match {
      case "day" | "days" => n.days
      case "month" | "months" => n.months
    }
  }

  /*
   * Internal method for loading and merging the user config file + the default config
   * Explanation for the type parameter: https://github.com/pureconfig/pureconfig/issues/358
   *   It could be added to the trait itself, but the type is only needed by this method, so for
   *   now I think we can leave it here.
   */
  protected[etl] def loadConfigWithDefaults[C <: Config : ClassTag : ConfigReader](
    configPath: String,
    defaultsPath: String,
    env: String): C = {

    val defaultConfig = ConfigFactory.parseResources(defaultsPath).resolve.getConfig(env)
    val config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve.withFallback(defaultConfig).resolve
    pureconfig.loadConfigOrThrow[C](config)
  }
}
