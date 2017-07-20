package fr.polytechnique.cmap.cnam.etl.config

trait CaseClassConfig {

  def fieldsToString: String = {
    this.getClass.getDeclaredFields.collect {
      case f if f.getName.head != '$' =>
        f.setAccessible(true)
        s"  ${f.getName} -> ${f.get(this)}"
    }.mkString("{\n", "\n", "\n}")
  }
}
