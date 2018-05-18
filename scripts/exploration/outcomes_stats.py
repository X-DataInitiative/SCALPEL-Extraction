import pandas as pd
import seaborn as sns

from exploration.utils import add_information_to_axe, patch_dates_axes


class OutcomeStats(object):

    def __init__(self, outcomes, outcome_name):
        self.outcomes = outcomes
        self.outcome_name = outcome_name

    def _plot_outcomes_per_day(self, ax, time_series):
        data = self.outcomes.groupBy("start").count().toPandas().sort_values(
            "start")

        if time_series:
            ax.plot(data.start, data["count"],
                    color=sns.xkcd_rgb["pumpkin orange"])
        else:
            data = data.set_index("start")

            idx = pd.date_range(data.index.min(), data.index.max())
            data.index = pd.DatetimeIndex(data.index)
            data = data.reindex(idx, fill_value=0)
            ax.bar(data.index, data["count"],
                   color=sns.xkcd_rgb["pumpkin orange"])

        patch_dates_axes(ax)

        add_information_to_axe(
            ax,
           "Distribution des evenements de {} par jour".format(self.outcome_name),
           "Date",
           "Nombre de {}".format(self.outcome_name)
        )

        return ax

    def plot_outcomes_per_day_time_series(self, ax):
        return self._plot_outcomes_per_day(ax, True)

    def plot_outcomes_per_day_as_bars(self, ax):
        return self._plot_outcomes_per_day(ax, False)
