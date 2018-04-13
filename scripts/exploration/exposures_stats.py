import matplotlib.pyplot as plt
import numpy as np
import pyspark.sql.functions as fn
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.backends.backend_pdf import PdfPages
from pyspark.sql import DataFrame
from seaborn import barplot

from exploration.utils import add_information_to_axe, add_percentage_axe


class ExposuresStats(object):

    def __init__(self, exposures: DataFrame):
        self.exposures = exposures.withColumn(
            "duration",
            fn.datediff(exposures.end, exposures.start)
        )
        self.exposures = self.exposures.withColumn(
            "duration_months",
            fn.bround(fn.col("duration") / 30)
        ).cache()

    def plot_exposures_mean_duration(self, ax: Axes):
        data = self.exposures.groupBy("value").mean().toPandas().sort_values("value")
        barplot(data=data, y="value", x="avg(duration)", ax=ax, orient="h", color=sns.xkcd_rgb["orange"])
        add_information_to_axe(ax, "Durée moyenne d'expositions", "Durée en jour",
                               "Type d'exposition")
        return ax

    def plot_duration_distribution(self, ax):
        data = self.exposures.groupby(["duration_months"]).count().toPandas()
        data.duration_months = data.duration_months.astype(np.int64)
        sns.barplot(data=data, x="duration_months", y="count",
                    color=sns.xkcd_rgb["orange"])
        total = data["count"].sum()
        add_information_to_axe(
            ax,
            "Distribution durée des expositions",
            "Durée en mois",
            "Nombre d'expositions"
        )
        add_percentage_axe(ax, total=total, y_limit=30)
        return ax

    def plot_per_class_duration_distribution(self):
        data = self.exposures.groupby(["duration_months", "value"]).count().toPandas()
        data.duration_months = data.duration_months.astype(np.int64)
        figures = []
        for value in data.value.unique():
            sub_data = data[data.value == value].copy()
            fig = plt.figure(figsize=(8, 5))
            ax = plt.gca()
            sns.barplot(data=sub_data, x="duration_months", y="count",
                        color=sns.xkcd_rgb["orange"], ax=ax)
            total = sub_data["count"].sum()

            add_information_to_axe(
                ax,
                "Durée des expositions de type {}".format(value),
                "Durée en mois",
                "Nombre d'expositions"
            )
            add_percentage_axe(ax, total=total, y_limit=30)
            plt.tight_layout()
            figures.append(fig)

        return figures


def save_exposures_stats(path: str, exposures) -> None:
    exposures_stats = ExposuresStats(exposures)

    with PdfPages(path) as pdf:
        fig = plt.figure(figsize=(8, 5))
        ax = plt.gca()
        exposures_stats.plot_exposures_mean_duration(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure(figsize=(8, 5))
        ax = plt.gca()
        exposures_stats.plot_duration_distribution(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        figures = exposures_stats.plot_per_class_duration_distribution()
        [pdf.savefig(fig) for fig in figures]
