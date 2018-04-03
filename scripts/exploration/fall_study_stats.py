import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.axes import Axes
from matplotlib.backends.backend_pdf import PdfPages

from exploration.outcomes_stats import OutcomeStats
from exploration.utils import add_information_to_axe, millify, patch_dates_axes


class TherapeuticStats(object):

    def __init__(self, event_df, colors_dict, event_type):
        self.stats = event_df.groupby("value").count().toPandas()
        self.colors_dict = colors_dict
        self.event_type = event_type

    def plot(self):
        palette = self.stats.sort_values("value").value.map(lambda x: self.colors_dict[x])
        ax = sns.barplot(data=self.stats.sort_values("value"), x="value", y="count",
                         palette=palette)
        ax.set_title("Distribution de {} au niveau thérapeutique".format(self.event_type))
        ax.set_xlabel("Classes thérapeutiques")
        ax.set_ylabel("Nombre de {}".format(self.event_type))
        ax.yaxis.set_major_formatter(millify)
        return ax


class PharmaStats(object):

    def __init__(self, molecule_mapping: pd.DataFrame, event_df, event_type,
                 colors_dict):
        counting_df = event_df.groupby("value").count().toPandas()
        self.stats = pd.merge(
            counting_df,
            molecule_mapping[["therapeutic", "pharmaceutic_family"]].drop_duplicates(),
            left_on="value",
            right_on="pharmaceutic_family", how='inner'
        )
        self.event_type = event_type
        self.colors_dict = colors_dict

    def plot(self):
        t = self.stats.sort_values("value")
        palette = t.therapeutic.map(lambda x: self.colors_dict[x])

        patches = [mpatches.Patch(color=self.colors_dict[therapeutic_class],
                                  label=therapeutic_class)
                   for therapeutic_class in t["therapeutic"].drop_duplicates().values]

        ax = sns.barplot(data=t, y="value", x="count",
                         orient="h", palette=palette, saturation=1.0)

        ax.set_xticklabels(t.value.values)

        ax.set_ylabel("Classes pharmaceutiques")
        ax.set_xlabel("Nombre de {}".format(self.event_type))
        ax.xaxis.set_major_formatter(millify)

        ax.set_title("Nombre de {} par classe pharmaceutique".format(self.event_type))
        plt.legend(handles=patches)

        return ax


class MoleculeStats(object):

    def __init__(self, molecule_mapping: pd.DataFrame, event_df, colors_dict,
                 event_type):
        counting_df = event_df.groupby("value").count().toPandas()
        self.stats = pd.merge(counting_df, molecule_mapping, left_on="value",
                              right_on="molecule", how='inner')

        self.event_type = event_type
        self.colors_dict = colors_dict
        self.pharma_dict = dict()

        hatches = ['*', '|||', '+', 'x', "\\", '$']
        for (key, value) in self.colors_dict.items():
            s = (molecule_mapping[molecule_mapping.therapeutic == key]
                 .sort_values("pharmaceutic_family")[
                     ["therapeutic", "pharmaceutic_family"]]
                 .drop_duplicates()
                 )
            self.pharma_dict.update({pharma_class: hatches[i] for i, pharma_class in
                                     enumerate(s.pharmaceutic_family.values)})

    def _plot(self, t):
        palette = t.therapeutic.map(lambda x: self.colors_dict[x])

        patches = [mpatches.Patch(facecolor=self.colors_dict[therapeutic_class],
                                  label=pharma_class,
                                  hatch=self.pharma_dict[pharma_class])
                   for therapeutic_class, pharma_class in
                   t[["therapeutic", "pharmaceutic_family"]].drop_duplicates().values]

        ax = sns.barplot(data=t, y="molecule", x="count", orient="h", palette=palette,
                         saturation=1.0)

        #ax.set_yticklabels(t.value.values)
        # Loop over the bars
        for text, thisbar in zip(ax.get_yticklabels(), ax.patches):
            # Set a different hatch for each bar
            thisbar.set_hatch(self.pharma_dict[t[
                t.molecule == text.get_text()].pharmaceutic_family.values[0]])

        ax.set_ylabel("Molecules")
        ax.set_xlabel("Nombre de {}".format(self.event_type))
        ax.xaxis.set_major_formatter(millify)
        plt.legend(handles=patches, loc='center left', bbox_to_anchor=(1, 0.5))
        plt.tight_layout()
        return ax

    def plot_overall_top_molecules(self, top=20):
        t = self.stats.sort_values("count")[-top:]
        ax = self._plot(t)
        ax.set_title("Top molecules selon {}".format(self.event_type))
        return ax

    def plot_top_of_therapeutic_classes(self, top=5):
        t = self.stats.groupby("therapeutic", as_index=False).apply(
            lambda x: x.sort_values("count", ascending=False)[:top])
        
        ax = self._plot(t)
        ax.set_title(
            "Top molécules chaque classe Thérapeutique selon {}".format(self.event_type))
        return ax


class FracturesStats(OutcomeStats):

    def __init__(self, fractures):
        OutcomeStats.__init__(self, fractures, "fractures")

    def plot_fractures_by_site_distribution(self, ax: Axes) -> Axes:
        fractures_site = self.outcomes.groupBy("groupID").count().toPandas()
        sns.barplot(data=fractures_site.sort_values("count", ascending=False),
                    y="groupID", x="count", color=sns.xkcd_rgb["pumpkin orange"], ax=ax)
        ax.grid(True, which="major", axis="x")
        ax.set_xlabel("Nombre de fractures")
        ax.set_ylabel("Partie du corps")
        ax.set_title("Distribution des fractures selon le site")

        return ax

    def plot_fractures_per_admission(self, ax: Axes) -> Axes:
        data = self.outcomes.groupBy(["patientID", "start"]).count().toPandas().sort_values(
            "start")
        sns.countplot(x="count", data=data, ax=ax)
        ax.grid(True, which="major", axis="y")
        add_information_to_axe(ax, "Distribution de nombre de fractures par admission",
                               "Nombre de fractures", "Nombre d'admission")
        return ax

    def plot_fracture_admission_number_per_patient(self, ax: Axes) -> Axes:
        data = self.outcomes.groupBy(["patientID", "start"]).count().toPandas().sort_values(
            "start").groupby("patientID").count()
        sns.countplot(data=data, x="start", ax=ax)
        ax.grid(True, which="major", axis="y")
        add_information_to_axe(ax, "Distribution de nombre d'admission pour fracture",
                               "Nombre d'admission pour fracture", "Nombre de patients")
        return ax

    def plot_admission_per_day(self, ax: Axes) -> Axes:
        data = self.outcomes.groupBy(["patientID", "start"]).count().toPandas().sort_values(
            "start").groupby("start")["patientID"].count().reset_index().set_index("start")

        idx = pd.date_range(data.index.min(), data.index.max())
        data.index = pd.DatetimeIndex(data.index)
        data = data.reindex(idx, fill_value=0)
        ax.bar(data.index, data["patientID"], color=sns.xkcd_rgb["pumpkin orange"])
        patch_dates_axes(ax)
        add_information_to_axe(ax, "Distribution de nombre d'admission pour fracture par jour",
                               "Date", "Nombre d'admission pour fracture")
        return ax


def save_fractures_stats(path: str, fractures) -> None:
    fractures_stats = FracturesStats(fractures)

    with PdfPages(path) as pdf:
        fig = plt.figure()
        ax = plt.gca()
        fractures_stats.plot_outcomes_per_day_as_bars(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure()
        ax = plt.gca()
        fractures_stats.plot_outcomes_per_day_time_series(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure()
        ax = plt.gca()
        fractures_stats.plot_fractures_by_site_distribution(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure()
        ax = plt.gca()
        fractures_stats.plot_fractures_per_admission(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure()
        ax = plt.gca()
        fractures_stats.plot_fracture_admission_number_per_patient(ax)
        plt.tight_layout()
        pdf.savefig(fig)

        fig = plt.figure()
        ax = plt.gca()
        fractures_stats.plot_admission_per_day(ax)
        plt.tight_layout()
        pdf.savefig(fig)
