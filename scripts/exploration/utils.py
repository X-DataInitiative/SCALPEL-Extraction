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


def add_percentage_axe(ax, total, y_limit=100, ):
    # Make twin axis
    ax2 = ax.twinx()

    # Switch so count axis is on right, frequency on left
    ax2.yaxis.tick_left()
    ax.yaxis.tick_right()

    # Also switch the labels over
    ax.yaxis.set_label_position('right')
    ax2.yaxis.set_label_position('left')

    ax2.set_ylabel('Pourcentage sur Population total [%]')

    # Use a LinearLocator to ensure the correct number of ticks
    ax.yaxis.set_major_locator(ticker.LinearLocator(11))

    # Fix the frequency range to 0-100
    ax2.set_ylim(0, y_limit)
    ax.set_ylim(0, (y_limit / 100) * total)
    ax.yaxis.set_major_formatter(millify)
    # And use a MultipleLocator to ensure a tick spacing of 10
    ax2.yaxis.set_major_locator(ticker.MultipleLocator(10))

    ax2.grid(None)

    ax.text(0.15, 0.85, 'Population Total : {:,}'.format(total),
            verticalalignment='top', horizontalalignment='left',
            transform=plt.gcf().transFigure,
            bbox={'alpha': 0.1, 'pad': 10})

    plt.tight_layout()

    
def add_x_percentage_axe(ax, total, x_limit=100, ):
    # Make twin axis
    ax2 = ax.twiny()

    ax2.xaxis.tick_top()
    ax.xaxis.tick_bottom()

    # Also switch the labels over
    ax.xaxis.set_label_position('bottom')
    ax2.xaxis.set_label_position('top')

    ax2.set_xlabel('Pourcentage sur Population total [%]')

    # Use a LinearLocator to ensure the correct number of ticks
    ax.xaxis.set_major_locator(ticker.LinearLocator(x_limit//5 + 1))

    # Fix the frequency range to 0-100
    ax2.set_xlim(0, x_limit)
    ax.set_xlim(0, (x_limit / 100) * total)
    ax.xaxis.set_major_formatter(millify)
    # And use a MultipleLocator to ensure a tick spacing of 10
    ax2.xaxis.set_major_locator(ticker.MultipleLocator(5))

    ax2.grid(None)

    #ax.text(0.15, 0.85, 'Population Total : {:,}'.format(total),
    #        verticalalignment='top', horizontalalignment='left',
    #        transform=plt.gcf().transFigure,
    #        bbox={'alpha': 0.1, 'pad': 10})
