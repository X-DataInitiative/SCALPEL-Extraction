import math

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib import ticker
from matplotlib.axes import Axes
from pyspark.sql import DataFrame, SQLContext, SparkSession


def read_data_frame(filepath: str) -> DataFrame:
    return (SQLContext
            .getOrCreate(SparkSession.builder.getOrCreate())
            .read.parquet(filepath).cache())


millnames = ['', ' K', ' M', ' Mi', ' Tr']


@ticker.FuncFormatter
def millify(x, pos):
    x = float(x)
    millidx = max(0, min(len(millnames) - 1,
                         int(math.floor(0 if x == 0 else math.log10(abs(x)) / 3))))

    if millidx > 1:
        return '{:.1f}{}'.format(x / 10 ** (3 * millidx), millnames[millidx])
    else:
        return '{:.0f}{}'.format(x / 10 ** (3 * millidx), millnames[millidx])


def add_information_to_axe(ax, title, xlabel, ylabel):
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)


def patch_dates_axes(ax: Axes) -> Axes:
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
    plt.gcf().autofmt_xdate()

    return ax
