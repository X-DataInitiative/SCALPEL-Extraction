package fr.polytechnique.cmap.cnam.util


// Old version
object DrugEventsTransformerHelper {
  // It's a function instead of a method because it is supposed to be passed to the 'udf' function.
  // If it was a method, it would be necessary to partially apply it (i.e. udf(moleculeMapping _))
  val moleculeMapping = (molecule: String) => {
    molecule match {
      case "INSULINE HUMAINE"   |
           "INSULINE ASPART"    |
           "INSULINE LISPRO"    |
           "INSULINE GLARGINE"  |
           "INSULINE BOVINE"    |
           "INSULINE DETEMIR"   |
           "INSULINE GLULISINE" |
           "INSULINE PORCINE"   |
           "INSULINE LISPRO (PROTAMINE)" => "INSULINE"
      case "DULAGLUTIDE"   |
           "CARBUTAMIDE"   |
           "VILDAGLIPTINE" |
           "ASSOCIATIONS"  |
           "MIGLITOL"      |
           "REPAGLINIDE"   |
           "LIRAGLUTIDE"   |
           "ACARBOSE"      |
           "EXENATIDE"     |
           "SAXAGLIPTINE"  |
           "SITAGLIPTINE"   => "OTHER"
      case "GLIMEPIRIDE"  |
           "GLIBORNURIDE" |
           "TOLBUTAMIDE"  |
           "GLICLAZIDE"   |
           "GLIPIZIDE"    |
           "SULFAMIDES"   |
           "GLIBENCLAMIDE" => "SULFONYLUREA"
      case "METFORMINE"                  => "METFORMINE"
      case "PIOGLITAZONE"                => "PIOGLITAZONE"
      case "BENFLUOREX"                  => "BENFLUOREX"
      case "ROSIGLITAZONE"               => "ROSIGLITAZONE"
    }
  }
}


// New version
//object DrugEventsTransformerHelper {
//  // It's a function instead of a method because it is supposed to be passed to the 'udf' function.
//  // If it was a method, it would be necessary to partially apply it (i.e. udf(moleculeMapping _))
//  val moleculeMapping = (molecule: String) => {
//    molecule match {
//      case "INSULINE HUMAINE"   |
//           "INSULINE ASPART"    |
//           "INSULINE LISPRO"    |
//           "INSULINE GLARGINE"  |
//           "INSULINE BOVINE"    |
//           "INSULINE DETEMIR"   |
//           "INSULINE GLULISINE" |
//           "INSULINE PORCINE"   |
//           "INSULINE LISPRO (PROTAMINE)" |
//           "ASSOCIATIONS" => "INSULINE"
//      case "ACARBOSE"      |
//           "MIGLITOL"      |
//           "DULAGLUTIDE"   |
//           "REPAGLINIDE"   |
//           "LIRAGLUTIDE"   |
//           "EXENATIDE"     |
//           "VILDAGLIPTINE" |
//           "SAXAGLIPTINE"  |
//           "SITAGLIPTINE"   => "OTHER"
//      case "GLIMEPIRIDE"  |
//           "GLIBORNURIDE" |
//           "TOLBUTAMIDE"  |
//           "GLICLAZIDE"   |
//           "GLIPIZIDE"    |
//           "SULFAMIDES"   |
//           "CARBUTAMIDE"  |
//           "GLIBENCLAMIDE" => "SULFONYLUREA"
//      case "METFORMINE"                  => "METFORMINE"
//      case "PIOGLITAZONE"                => "PIOGLITAZONE"
//      case "BENFLUOREX"                  => "BENFLUOREX"
//      case "ROSIGLITAZONE"               => "ROSIGLITAZONE"
//    }
//  }
//}