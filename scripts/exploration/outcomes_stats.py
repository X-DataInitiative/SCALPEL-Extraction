import matplotlib.dates as mdates
import seaborn as sns
from matplotlib import pyplot

from exploration.utils import add_information_to_axe


class OutcomeStats(object):

    def __init__(self, outcomes, outcome_name):
        self.outcomes = outcomes
        self.outcome_name = outcome_name

    def _plot_outcomes_per_day(self, ax, time_series):
        distrib_jour = self.outcomes.groupBy("start").count().toPandas().sort_values(
            "start")

        if time_series:
            ax.plot(distrib_jour.start, distrib_jour["count"],
                    color=sns.xkcd_rgb["pumpkin orange"])
        else:
            ax.bar(distrib_jour.start, distrib_jour["count"],
                   color=sns.xkcd_rgb["pumpkin orange"])

        major = mdates.MonthLocator()  # every year
        minor = mdates.MonthLocator()  # every week-debut
        date_format = mdates.DateFormatter('%b')

        # format the ticks
        ax.xaxis.set_major_locator(major)
        ax.xaxis.set_major_formatter(date_format)
        ax.xaxis.set_minor_locator(minor)

        ax.grid(True, which="major", axis="y")
        ax.grid(True, which="minor", axis="x", linestyle='--')
        # rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        pyplot.gcf().autofmt_xdate()

        add_information_to_axe(ax, "Nombre de {}".format(self.outcome_name), "Date",
                               "Distribution des evenements de {} par jour".format(
                                   self.outcome_name))

        return ax

    def plot_outcomes_per_day_time_series(self, ax):
        return self._plot_outcomes_per_day(ax, True)

    def plot_outcomes_per_day_as_bars(self, ax):
        return self._plot_outcomes_per_day(ax, False)
