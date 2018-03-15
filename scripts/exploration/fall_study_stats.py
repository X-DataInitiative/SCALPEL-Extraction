import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from exploration.utils import millify


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
                 .sort_values("pharmaceutic_family")[["therapeutic", "pharmaceutic_family"]]
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

        ax = sns.barplot(data=t, y="PHA_ATC_C07", x="count", orient="h", palette=palette,
                         saturation=1.0)

        ax.set_yticklabels(t.value.values)
        # Loop over the bars
        for text, thisbar in zip(ax.get_yticklabels(), ax.patches):
            # Set a different hatch for each bar
            thisbar.set_hatch(self.pharma_dict[t[
                t.molecule == text.get_text()].pharmaceutic_family.values[0]])

        ax.set_ylabel("Molecules")
        ax.set_xlabel("Nombre de {}".format(self.event_type))
        ax.xaxis.set_major_formatter(millify)
        plt.legend(handles=patches)

        return ax

    def plot_overall_top_molecules(self, top=20):
        t = self.stats.sort_values("count")[-top:].sort_values(["pharmaceutic_family", "value"])
        ax = self._plot(t)
        ax.set_title("Top molecules selon {}".format(self.event_type))
        return ax

    def plot_top_of_therapeutic_classes(self, top=5):
        t = self.stats.groupby("therapeutic", as_index=False).apply(
            lambda x: x.sort_values("count", ascending=False)[:top])
        ax = self._plot(t)
        ax.set_title("Top molécules chaque classe Thérapeutique selon {}".format(self.event_type))
        return ax


