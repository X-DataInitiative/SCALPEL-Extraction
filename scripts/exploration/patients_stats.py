from datetime import datetime
from os import path

import matplotlib.pyplot as plt
from matplotlib import ticker
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import seaborn as sns
from IPython.display import display
from pyspark.sql import functions as fn

from exploration.utils import millify


class MyPatientsDF(object):

    def __init__(self, patients, cohort_name, reference_date=datetime(2015, 1, 1),
                 max_age=150):
        self.patients = patients
        self.cohort_name = cohort_name
        self.reference_date = reference_date
        self.bucket_mapping = self._get_string_maps(max_age)
        self.color_age_bucket_mapping = self._get_color_maps(max_age)

    @staticmethod
    def _get_color_maps(max_age):
        age_lists = range(0, max_age, 5)
        size = len(age_lists)
        buckets = zip(age_lists[:-1], age_lists[1:])
        palette = sns.color_palette("Paired", n_colors=size)
        return {"[{}, {}[".format(bucket[0], bucket[1]): palette[i] for (
            i, bucket) in enumerate(buckets)}

    def get_age_bucket_colors(self, buckets):
        return [self.color_age_bucket_mapping[bucket] for bucket in buckets]

    @staticmethod
    def _get_string_maps(max_age):
        age_lists = range(0, max_age, 5)
        buckets = zip(age_lists[:-1], age_lists[1:])
        string_maps = {i: "[{}, {}[".format(bucket[0], bucket[1]) for (
            i, bucket) in enumerate(buckets)}
        return string_maps

    def _add_reference_date(self):
        self.patients = (
            self.patients.withColumn("referenceDate", fn.coalesce(
                fn.col("deathDate"), fn.lit(self.reference_date)))
        )

    def _add_age_in_months(self):
        try:
            self.patients = self.patients.withColumn(
                "ageInMonths",
                fn.months_between(fn.col("referenceDate"), fn.col("birthDate")))
        except:
            self._add_reference_date()
            self.patients = self.patients.withColumn(
                "ageInMonths",
                fn.months_between(fn.col("referenceDate"), fn.col("birthDate")))

    def add_age(self):
        try:
            self.patients = self.patients.withColumn(
                "age", fn.expr("ageInMonths div 12"))
        except:
            self._add_age_in_months()
            self.patients = self.patients.withColumn(
                "age", fn.expr("ageInMonths div 12"))

    def add_age_bucket(self):
        try:
            self.patients = self.patients.withColumn(
                "ageBucket", fn.expr("ageInMonths div (12*5)"))
        except:
            self._add_age_in_months()
            self.patients = self.patients.withColumn(
                "ageBucket", fn.expr("ageInMonths div (12*5)"))


class PatientStatsBuilder(object):
    def __init__(self, patients_df: MyPatientsDF):
        self.patients_df = patients_df

    def get_distribution_by_age_bucket(self):
        agg_df = self.patients_df.patients.groupBy("ageBucket").count().alias(
            "num_patients").orderBy("ageBucket").toPandas()
        agg_df.ageBucket = agg_df.ageBucket.map(
            lambda x: self.patients_df.bucket_mapping[x])
        return agg_df

    def get_distribution_by_gender(self):
        return self.patients_df.patients.groupBy("gender").count().alias(
            "num_patients").toPandas()

    def get_distribution_by_gender_age_bucket(self):
        agg_df = self.patients_df.patients.groupBy("gender", "ageBucket").count().alias(
            "num_patients").orderBy("gender", "ageBucket").toPandas()
        agg_df.ageBucket = agg_df.ageBucket.map(
            lambda x: self.patients_df.bucket_mapping[x])
        return agg_df


class PatientStatsPlotter(object):
    def __init__(self, stats: PatientStatsBuilder, total=None):
        self.stats = stats
        self.total = total

    def _plot_proportion(self, ax):
        font = {'size': 14}
        for p in ax.patches:
            height = p.get_height()
            nh = (height / self.total) * 100
            if not np.isfinite(nh):
                nh = 0
                height = 0

            ax.text(p.get_x() + p.get_width() / 2.,
                    height + 0.3,
                    '{:1.2f}%'.format(nh),
                    ha="center", fontdict=font)

    def _patch_axe(self, ax, y_limit=100):
        # Make twin axis
        ax2 = ax.twinx()

        # Switch so count axis is on right, frequency on left
        ax2.yaxis.tick_left()
        ax.yaxis.tick_right()

        # Also switch the labels over
        ax.yaxis.set_label_position('right')
        ax2.yaxis.set_label_position('left')

        ax2.set_ylabel('Pourcentage sur Population total [%]')

        #for p in ax.patches:
            #x = p.get_bbox().get_points()[:, 0]
            #y = p.get_bbox().get_points()[1, 1]
            #ax.annotate('{:.0f}'.format(y), (x.mean(), y),
            #            ha='center', va='bottom', rotation=90)

        # Use a LinearLocator to ensure the correct number of ticks
        ax.yaxis.set_major_locator(ticker.LinearLocator(11))

        # Fix the frequency range to 0-100
        ax2.set_ylim(0, y_limit)
        ax.set_ylim(0, (y_limit / 100) * self.total)
        ax.yaxis.set_major_formatter(millify)
        # And use a MultipleLocator to ensure a tick spacing of 10
        ax2.yaxis.set_major_locator(ticker.MultipleLocator(10))

        ax2.grid(None)

        ax.text(0.15, 0.85, 'Population Total : {:,}'.format(self.total),
                verticalalignment='top', horizontalalignment='left',
                transform=plt.gcf().transFigure,
                bbox={'alpha': 0.1, 'pad': 10})

        plt.tight_layout()

    def distribution_by_age_bucket(self, ax):
        df = self.stats.get_distribution_by_age_bucket()
        colors = self.stats.patients_df.get_age_bucket_colors(df.ageBucket.values.tolist())
        ax = sns.barplot(x="ageBucket", data=df, y="count", ax=ax, palette=colors)

        ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
        ax.set_ylabel("Nombre de patients")
        ax.set_xlabel("Tranche d'age")
        ax.set_title("Distribution des {}\nsuivant la tranche d'age".format(
            self.stats.patients_df.cohort_name))
        if self.total:
            self._patch_axe(ax, y_limit=25)

        return ax

    def distribution_by_gender(self, ax):
        df = self.stats.get_distribution_by_gender()
        ax = sns.barplot(x="gender", data=df, y="count", ax=ax)
        ax.set_xticklabels(["Homme", "Femme"])
        ax.set_ylabel("Nombre de patients")
        ax.set_xlabel("Genre")
        ax.set_title("Distribution des {}\nsuivant le genre".format(
            self.stats.patients_df.cohort_name))
        if self.total:
            self._patch_axe(ax)

        return ax

    def distribution_by_gender_age_bucket(self, ax):
        df = self.stats.get_distribution_by_gender_age_bucket()
        ax = sns.barplot(x="ageBucket", y="count",
                         hue="gender", data=df, ax=ax)
        ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
        ax.set_ylabel("Nombre de patients")
        ax.set_xlabel("Tranche d'age")
        ax.set_title("Distribution des {}\nsuivant le genre et la tranche d'age".format(
            self.stats.patients_df.cohort_name))

        gender_mapping = {1: "Homme", 2: "Femme"}
        ax.legend(loc=1, title="Genre")
        legend = ax.get_legend()
        [label.set_text(gender_mapping[int(label.get_text())])
         for label in legend.get_texts()]
        if self.total:
            self._patch_axe(ax, y_limit=25)
        return ax


def do_show_stats(patients, cohort_name):
    patients_df = MyPatientsDF(patients, cohort_name)
    patients_df.add_age_bucket()
    stats = PatientStatsBuilder(patients_df)
    print("Distribution by age bucket of {} patients.".format(
        stats.patients_df.cohort_name))
    display(stats.get_distribution_by_age_bucket())

    print("Distribution by gender of {} patients.".format(
        stats.patients_df.cohort_name))
    display(stats.get_distribution_by_gender())

    print("Distribution by gender and age bucket of {} patients.".format(
        stats.patients_df.cohort_name))
    display(stats.get_distribution_by_gender_age_bucket())


def do_plot_stats(patients, cohort_name):
    patients_df = MyPatientsDF(patients, cohort_name)
    patients_df.add_age_bucket()
    stats = PatientStatsBuilder(patients_df)
    stats_plotter = PatientStatsPlotter(stats)
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(18, 9))

    stats_plotter.distribution_by_gender(axes[0])
    stats_plotter.distribution_by_age_bucket(axes[1])
    stats_plotter.distribution_by_gender_age_bucket(axes[2])

    plt.tight_layout()


def save_patients_stats(patients, cohort_name, total, root_path):
    my_patients_df = MyPatientsDF(patients, cohort_name)
    my_patients_df.add_age_bucket()
    stats = PatientStatsBuilder(my_patients_df)
    plotStats = PatientStatsPlotter(stats, total)

    file_path = path.join(root_path, '{}_stats.pdf'.format(cohort_name))
    with PdfPages(file_path) as pdf:
        fig = plt.figure()
        ax = plt.gca()
        plotStats.distribution_by_gender(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure()
        ax = plt.gca()
        plotStats.distribution_by_age_bucket(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure()
        ax = plt.gca()
        plotStats.distribution_by_gender_age_bucket(ax)
        plt.tight_layout()
        pdf.savefig(fig)
