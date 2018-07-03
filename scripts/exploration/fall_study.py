from collections import namedtuple

import pandas as pd


def fall_study_mapping(IRPHAR_table: str, outpath: str):
    """
    The function reads the IRPHAR table from the specified location and outputs the mapped molucules in Csv format.
    Parameters
    ----------
    IRPHAR_table:
        The filepath for the IRPHAR data table
    outpath:
        The path where the resultant graph is to be saved

    Returns
        The mapped molecules in csv format.
    -------

    """
    Pharma_mapping = namedtuple('Pharam_Mapping', ['Pharma_class', 'ATC_Codes', 'ATC_Exceptions', "Therapeutic_class"])
    fall_study_mappings = [
        Pharma_mapping("Antidepresseurs_Tricycliques", ["N06AA"], ["N06AA06"], "Antidepresseurs"),
        Pharma_mapping("Antidepresseurs_ISRS", ["N06AB"], None, "Antidepresseurs"),
        Pharma_mapping("Antidepresseurs_ISRSN", ["N06AX11", "N06AX16", "N06AX17", "N06AX21", "N06AX26"], None,
                       "Antidepresseurs"),
        Pharma_mapping("Antidepresseurs_IMAO_AB", ["N06AF"], None, "Antidepresseurs"),
        Pharma_mapping("Antidepresseurs_IMAO_A", ["N06AG"], None, "Antidepresseurs"),
        Pharma_mapping("Antidepresseurs_Autres", ["N06AX03", "N06AX09", "N06AX14", "N06AX22", "N06AA06"], None,
                       "Antidepresseurs"),
        Pharma_mapping("Antihypertenseurs_SARTANS", ["C09C", "C09D"], None, "Antihypertenseurs"),
        Pharma_mapping("Antihypertenseurs_IEC", ["C09A", "C09B"], None, "Antihypertenseurs"),
        Pharma_mapping("Antihypertenseurs_Diuretiques", ["C03"], None, "Antihypertenseurs"),
        Pharma_mapping("Antihypertenseurs_Betabloquants", ["C07"], None, "Antihypertenseurs"),
        Pharma_mapping("Antihypertenseurs_Inhibiteurs_calciques", ["C08"], None, "Antihypertenseurs"),
        Pharma_mapping("Antihypertenseurs_Autres", ["C02", "C09XA", "C10BX03"], None, "Antihypertenseurs"),
        Pharma_mapping("Hypnotiques_Benzodiazepines_anxiolytiques", ["N05BA"], None, "Hypnotiques"),
        Pharma_mapping("Hypnotiques_Autres_anxiolytiques", ["N05BB", "N05BC", "N05BE", "N05BX"], ["N05BC51"],
                       "Hypnotiques"),
        Pharma_mapping("Hypnotiques_Benzodiazepines_hypnotiques", ["N05CD"], ["N05CD08"], "Hypnotiques"),
        Pharma_mapping("Hypnotiques_Autres_hypnotiques", ["N05CF", "N05BC51", "N05CM11", "N05CM16", "N05CX"], None,
                       "Hypnotiques"),
        Pharma_mapping("Neuroleptiques_Neuroleptiques_atypiques", ["N05A"],
                       ["N05AL06", "N05AN01", "N05AA", "N05AH02", "N05AH03", "N05AL05", "N05AX08", "N05AX12",
                        "N05AA07"], "Neuroleptiques"),
        Pharma_mapping("Neuroleptiques_Autres_neuroleptiques",
                       ["N05AA", "N05AH02", "N05AH03", "N05AL05", "N05AX08", "N05AX12"], None, "Neuroleptiques")
    ]

    ir_pha_r = pd.read_csv(IRPHAR_table, sep=";", index_col="Unnamed: 0", na_values="NaN")

    # Keep only the lines with PHA_NOM_PA
    ir_pha_r = ir_pha_r[ir_pha_r.PHA_NOM_PA.notnull()].copy()
    input_table = ir_pha_r[["PHA_ATC_C07", "PHA_CIP_C13", "PHA_NOM_PA"]].copy()

    def find_pharma_class(ATC_code: str, mappings) -> str:
        for pharma_mapping in mappings:
            found = any([ATC_code.startswith(atc_code) for atc_code in pharma_mapping.ATC_Codes])

            if found:
                exception = (pharma_mapping.ATC_Exceptions is not None) and any(
                    [ATC_code.startswith(atc_code) for atc_code in pharma_mapping.ATC_Exceptions])
                if not exception:
                    return pharma_mapping.Pharma_class

        return None

    def find_thera_class(ATC_code: str, mappings) -> str:
        for pharma_mapping in mappings:
            found = any([ATC_code.startswith(atc_code) for atc_code in pharma_mapping.ATC_Codes])

            if found:
                exception = (pharma_mapping.ATC_Exceptions is not None) and any(
                    [ATC_code.startswith(atc_code) for atc_code in pharma_mapping.ATC_Exceptions])
                if not exception:
                    return pharma_mapping.Therapeutic_class

        return None

    input_table["pharmaceutic_family"] = input_table.PHA_ATC_C07.apply(
        lambda x: find_pharma_class(x, fall_study_mappings))
    input_table["therapeutic"] = input_table.PHA_ATC_C07.apply(lambda x: find_thera_class(x, fall_study_mappings))

    output_table = input_table[input_table["pharmaceutic_family"].notnull()].copy()

    # Split the Molecules
    molecules_series = output_table.set_index("PHA_CIP_C13").PHA_NOM_PA.apply(lambda x: x.split(" + "))

    # Flat Map
    molecules_series = molecules_series.apply(pd.Series).unstack().dropna().reset_index().drop("level_0",
                                                                                               axis="columns").rename(
        columns={0: "molecule"})

    final_df = pd.merge(output_table, molecules_series, on="PHA_CIP_C13", how="inner")
    final_df.to_csv(outpath, index=False)
