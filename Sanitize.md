# Sanitize

The SNDS sources used for the extraction are raw data from the CNAM. Thus, they need to be sanitized before any event extraction


We use some classical data quality rules for the SNDS. The details on the name of the variables and on these rules can be found (in french) on the [SNDS collaborative documentation website](https://documentation-snds.health-data-hub.fr/):

* [+ DCIR +]: 
    * `DPN_QLF` ≠ "71", remove the lines [for information only](https://documentation-snds.health-data-hub.fr/ressources/documents%20Cnam/faq/faq_dcir.html#dcir) : these lines correspond to services carried out during a stay or an outpatient consultation and sent for information. 
    * `BSE_PRS_NAT` ≠ "0", remove the lines without a proper *Nature de la prestation* 
* [+ PMSI +]:
  * For all PMSI products ([see details here](https://documentation-snds.health-data-hub.fr/fiches/depenses_hopital_public.html#valorisation-des-sejours-a-l-hopital-public)):
    * `ETA_NUM` $`\notin`$ finessDoublons
        remove information reported by geogaphic FINESS for APHP, HCL and APHM (duplicates for this information also goes back through legal FINESS.)
  * [+ MCO +]:
    * `SEJ_TYP` = $`\empty`$ OR `SEJ_TYP` ≠ "B" OR (`GRG_GHM` like "28%" AND `GRG_GHM` $`\notin`$ {"28Z14Z", "28Z15Z", "28Z16Z"}), (TODO : Why ?)
    * `GRG_GHM` ≠ "14Z08Z", remove IVG hospital stay (TODO : Why ? link to explanation documentation)
    * `GHS_NUM` ≠ "9999", remove non reimbursed hospital stays. 
    * {`SEJ_RET`, `FHO_RET`, `PMS_RET`, `DAT_RET`, `NIR_RET`, `SEX_RET`} = "0", remove corrupted patient id or sex.
    * `GRG_GHM` `\notlike` "90%", remove corrupted patient grouping codes.
  * [+ MCO_CE +]:
    * {`NAI_RET`, `IAS_RET`, `ENT_DAT_RET`, `NIR_RET`, `SEX_RET`} = "0", remove corrupted patient id or sex.
  * [+ SSR +]:
      * {`NIR_RET`, `SEJ_RET`, `FHO_RET`, `PMS_RET`, `DAT_RET`} = "0" (for [+ SSR +]), remove corrupted hospital stay, date or patient id.
      * `GRG_GME` `\notlike` "90%", remove corrupted patient grouping codes.
  * [+ SSR_CE +]:
    * {`NIR_RET`, `NAI_RET`, `SEX_RET`, `IAS_RET`, `ENT_DAT_RET`} ="0" (for [+ SSR_CE +]), remove corrupted hospital stay, date or patient id.

  * [+ HAD +]:
    * {`NIR_RET`, `SEJ_RET`, `FHO_RET`, `PMS_RET`, `DAT_RET`} = "0", remove corrupted hospital stay, date or patient id. 


## Details of the Finess doublons

```scala
val finessDoublons = List(
    //APHP 
    "600100093","600100101","620100016","640790150","640797098","750100018","750806226", 
    "750100356","750802845","750801524","750100067","750100075","750100042","750805228",
    "750018939","750018988","750100091","750100083","750100109","750833345","750019069",
    "750803306","750019028","750100125","750801441","750019119","750100166","750100141",
    "750100182","750100315","750019648","750830945","750008344","750803199","750803447",
    "750100216","750100208","750833337","750000358","750019168","750809576","750100299",
    "750041543","750100232","750802258","750803058","750803454","750100273","750801797",
    "750803371","830100012","830009809","910100015","910100031","910100023","910005529",
    "920100013","920008059","920100021","920008109","920100039","920100047","920812930",
    "920008158","920100054","920008208","920100062","920712551","920000122","930100052",
    "930100037","930018684","930812334","930811294","930100045","930011408","930811237",
    "930100011","940018021","940100027","940100019","940170087","940005739","940100076",
    "940100035","940802291","940100043","940019144","940005788","940100050","940802317",
    "940100068","940005838","950100024","950100016",
    //APHM                   
    "130808231","130809775","130782931",
    "130806003","130783293","130804305","130790330","130804297","130783236","130796873",
    "130808520","130799695","130802085","130808256","130806052","130808538","130802101",
    "130796550","130014558","130784234","130035884","130784259","130796279","130792856",
    "130017239","130792534","130793698","130792898","130808546","130789175","130780521",
    "130033996","130018229", 
    //HCL                   
    "90787460","690007422","690007539","690784186","690787429",
    "690783063","690007364","690787452","690007406","690787486","690784210","690799416",
    "690784137","690007281","690799366","690784202","690023072","690787577","690784194",
    "690007380","690784129","690029194","690806054","690029210","690787767","690784178",
    "690783154","690799358","690787817","690787742","690784152","690784145","690783121",
    "690787478","690007455","690787494","830100558","830213484"
    )
```