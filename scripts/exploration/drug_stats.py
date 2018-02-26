from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict
from itertools import product

import seaborn as sns
from matplotlib.ticker import ScalarFormatter, MultipleLocator
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from pyspark.sql import DataFrame
from pyspark.sql import functions as fn

from .patients_stats import MyPatientsDF
from .utils import read_data_frame


class DrugsDataFrame(MyPatientsDF):

    def __init__(self, patients: DataFrame, drugs: DataFrame,
                 cohort_name: str, max_age: int = 150,
                 reference_date=datetime(2015, 1, 1)):
        drugs_data_frame = drugs.join(patients, "patientID", "inner").cache()
        super().__init__(drugs_data_frame, cohort_name, reference_date, max_age)

    def group_by(self, *cols):
        return self.patients.groupby(*cols)


class DrugsStatsStrategy(ABC):

    @property
    @abstractmethod
    def drug_name(self) -> str:
        pass

    @property
    def DRUG_PURCHASE_COLUMN(self) -> str:
        return "drug_purchase"

    @property
    @abstractmethod
    def DRUG_PURCHASE_BUCKET_COLUMN(self) -> str:
        pass

    @property
    @abstractmethod
    def DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN(self) -> str:
        pass

    @property
    def age_mapping(self):
        age_lists = range(0, 250, 5)
        buckets = zip(age_lists[:-1], age_lists[1:])
        string_maps = {i: "[{}, {}[".format(bucket[0], bucket[1]) for (i, bucket) in
                       enumerate(buckets)}
        return string_maps

    @property
    @abstractmethod
    def boxes_mapping(self) -> Dict[int, str]:
        pass

    @property
    @abstractmethod
    def special_boxes_mapping(self) -> Dict[int, str]:
        pass

    @abstractmethod
    def count_box_to_bucket(self, number_boxes):
        pass

    @abstractmethod
    def count_box_to_special_box_buckets(self, number_boxes):
        pass

    def _strategy_1_ticks(self, ax):
        ax.xaxis.set_major_locator(MultipleLocator(5))
        ax.xaxis.set_major_formatter(ScalarFormatter())

        ax.set_xlabel("Nombre d'achat sur une annee pour un patient")

        ax.set_title(
            "Distribution de nombre d'achat de {} par an-patient".format(self.drug_name)
        )
        return ax

    @abstractmethod
    def _strategy_2_ticks(self, ax):
        pass

    @abstractmethod
    def _strategy_3_ticks(self, ax):
        pass

    def tweak_plot_ticks(self, ax, column):
        if column == self.DRUG_PURCHASE_COLUMN:
            return self._strategy_1_ticks(ax)
        elif column == self.DRUG_PURCHASE_BUCKET_COLUMN:
            return self._strategy_2_ticks(ax)
        elif column == self.DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN:
            return self._strategy_3_ticks(ax)
        else:
            print("Your column {} doesn't have corresponding strategy".format(column))

    def tweak_plot_legend(self, ax, column):
        mapping_to_use = self._get_mapping(column)

        legend = ax.get_legend()
        [label.set_text(mapping_to_use[int(label.get_text())]) for label in
         legend.get_texts()]

        return ax

    def _get_mapping(self, column) -> Dict[int, str]:
        if column == self.DRUG_PURCHASE_BUCKET_COLUMN:
            return self.boxes_mapping
        elif column == self.DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN:
            return self.special_boxes_mapping
        else:
            print("Your column {} doesn't have corresponding strategy".format(column))


class DrugPurchaseStatsBuilder(object):
    def __init__(self, patients: DataFrame, drugs: DataFrame,
                 stat_strategy: DrugsStatsStrategy):
        drugs_name = stat_strategy.drug_name
        target_drugs = drugs.where(fn.col("value") == drugs_name)
        self.drugs_data_frame = DrugsDataFrame(patients, target_drugs, drugs_name)
        self.drugs_data_frame.add_age_bucket()

        self.stats = (
            self.drugs_data_frame
                .group_by("patientID", "ageBucket", "gender")
                .count()
                .toPandas().rename(columns={"count": stat_strategy.DRUG_PURCHASE_COLUMN})
        )

        if stat_strategy.DRUG_PURCHASE_BUCKET_COLUMN is not None:
            r = self.stats[stat_strategy.DRUG_PURCHASE_COLUMN]. \
                apply(lambda x: stat_strategy.count_box_to_bucket(x))
            self.stats[stat_strategy.DRUG_PURCHASE_BUCKET_COLUMN] = r

        if stat_strategy.DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN is not None:
            s = self.stats[stat_strategy.DRUG_PURCHASE_COLUMN]. \
                apply(lambda x: stat_strategy.count_box_to_special_box_buckets(x))
            self.stats[stat_strategy.DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN] = s

        self.filtered_stats = self.stats[
            (self.stats.ageBucket < 20) & (self.stats.ageBucket > 12) & (
                    self.stats[stat_strategy.DRUG_PURCHASE_COLUMN] < 50)].copy()


class DrugPurchaseStatsPlotter(object):

    def __init__(self, drugs_stats: DrugPurchaseStatsBuilder,
                 stat_strategy: DrugsStatsStrategy):
        self.drugs_stats = drugs_stats
        self.stat_strategy = stat_strategy

    def _plot_distribution(self, x_axe, logscale=False, percentage=False):
        if percentage:
            ax = sns.barplot(x=x_axe, y=x_axe, data=self.drugs_stats.stats,
                             estimator=lambda x: len(x) / len(
                                 self.drugs_stats.stats) * 100)
            ax.set_ylabel("Pourcentage sur population affichee")
        else:
            ax = sns.countplot(x=x_axe, data=self.drugs_stats.stats)
            if logscale:
                ax.set_yscale("log", nonposy='clip')
                ax.set_ylabel("Nombre de patient en echelle logarithmique")
            else:
                ax.set_ylabel("Nombre de patient")

        return ax

    def distribution_box(self, logscale=False, percentage=False):
        ax = self._plot_distribution(self.stat_strategy.DRUG_PURCHASE_COLUMN, logscale,
                                     percentage)
        self.stat_strategy.tweak_plot_ticks(ax, self.stat_strategy.DRUG_PURCHASE_COLUMN)
        return ax

    def distribution_box_bucket_purchase(self, logscale=False, percentage=False):
        ax = self._plot_distribution(self.stat_strategy.DRUG_PURCHASE_BUCKET_COLUMN,
                                     logscale, percentage)
        self.stat_strategy.tweak_plot_ticks(
            ax,
            self.stat_strategy.DRUG_PURCHASE_BUCKET_COLUMN
        )
        return ax

    def distribution_special_bucket_purchase(self, logscale=False, percentage=False):
        ax = self._plot_distribution(
            self.stat_strategy.DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN,
            logscale,
            percentage
        )
        self.stat_strategy.tweak_plot_ticks(
            ax,
            self.stat_strategy.DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN
        )
        return ax

    def _box_bucket_by(self, by, box_column, logscale=False, percentage=False):
        if percentage:
            patient_counts = (
                self.drugs_stats.stats
                    .groupby([by])[box_column]
                    .value_counts(normalize=True)
                    .mul(100)
                    .reset_index()
                    .sort_values(by)
                    .rename(columns={0: 'percentage'})
            )
            ax = sns.barplot(x=by, y="percentage", hue=box_column,
                             data=patient_counts,
                             palette=sns.color_palette("Paired", n_colors=9))

            ax.set_ylabel("Pourcentage à l'interieur de la sous-population")

        else:
            ax = sns.countplot(x=by, hue=box_column,
                               data=self.drugs_stats.stats,
                               palette=sns.color_palette("Paired", n_colors=9))
            if logscale:
                ax.set_yscale("log", nonposy='clip')
                ax.set_ylabel("Nombre de patient en echelle logarithmique")
            else:
                ax.set_ylabel("Nombre de patient")

        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),
                  title="Nombre d'achat par\nan par patient")

        self.stat_strategy.tweak_plot_legend(ax, box_column)

        return ax

    def distribution_box_bucket_age_bucket(self, box_column, logscale=False,
                                           percentage=False):
        ax = self._box_bucket_by("ageBucket", box_column, logscale, percentage)

        x_tickslabels = [self.stat_strategy.age_mapping[int(tick.get_text())] for tick in
                         ax.get_xticklabels()]
        ax.set_xticklabels(x_tickslabels, rotation=90)
        ax.set_xlabel("Tranche d'age du patient")

        ax.set_title(
            "Distribution de nombre d'achat de {}\n"
            "par an-patient suivant les tranches d'age".format(
                self.stat_strategy.drug_name)
        )

        ax.set_xlabel("Tranche d'age du patient")
        return ax

    def distribution_box_bucket_gender(self, box_column, logscale=False, percentage=False):
        ax = self._box_bucket_by("gender", box_column, logscale, percentage)
        ax.set_xticklabels(["Homme", "Femme"])
        ax.set_xlabel("Genre")

        ax.set_title(
            "Distribution de nombre d'achat de {}\n"
            "par an-patient suivant le genre".format(
                self.stat_strategy.drug_name)
        )

        return ax


def save_drugs_stats(patients_path: str, drugs_path: str, strategy: DrugsStatsStrategy,
                     output_path: str):
    print("Reading patients")
    patients = read_data_frame(patients_path)
    print("Readings drugs")
    drugs = read_data_frame(drugs_path)
    print("Building stats")
    builder = DrugPurchaseStatsBuilder(patients, drugs, strategy)
    print("Building plotts")
    plotter = DrugPurchaseStatsPlotter(builder, strategy)

    with PdfPages(output_path) as pdf:
        for (percentage, logscale) in product([True, False], repeat=2):
            fig = plt.figure()
            plotter.distribution_box(percentage=percentage, logscale=logscale)
            plt.tight_layout()
            pdf.savefig(fig)

        for (percentage, logscale) in product([True, False], repeat=2):
            fig = plt.figure()
            plotter.distribution_box_bucket_purchase(percentage=percentage,
                                                     logscale=logscale)
            plt.tight_layout()
            pdf.savefig(fig)

        for (percentage, logscale) in product([True, False], repeat=2):
            fig = plt.figure()
            plotter.distribution_special_bucket_purchase(percentage=percentage,
                                                         logscale=logscale)
            plt.tight_layout()
            pdf.savefig(fig)

        box_bucket_columns = [strategy.DRUG_PURCHASE_BUCKET_COLUMN,
                              strategy.DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN]

        options = [_ for _ in
                   product(box_bucket_columns, product([True, False], repeat=2))]
        for (box_bucket_column, (logscale, percentage)) in options:
            fig = plt.figure()
            plotter.distribution_box_bucket_age_bucket(box_bucket_column, logscale,
                                                       percentage)
            plt.tight_layout()
            plt.subplots_adjust(right=0.75)
            pdf.savefig(fig)

        for (box_bucket_column, (logscale, percentage)) in options:
            fig = plt.figure()
            plotter.distribution_box_bucket_gender(box_bucket_column, logscale,
                                                   percentage)
            plt.tight_layout()
            plt.subplots_adjust(right=0.75)
            pdf.savefig(fig)
