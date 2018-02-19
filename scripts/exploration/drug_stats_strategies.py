from typing import List

from .drug_stats import DrugsStatsStrategy


class PharmaDrugsStrategy(DrugsStatsStrategy):

    def count_box_to_bucket(self, number_boxes):
        return number_boxes // 6

    def _strategy_2_ticks(self, ax):
        x_tickslabels = [self.boxes_mapping[int(tick.get_text())] for tick in
                         ax.get_xticklabels()]
        ax.set_xticklabels(x_tickslabels, rotation=90)
        ax.set_xlabel("Nombre d'achat sur une annee pour un patient")

        ax.set_title(
            "Distribution de nombre d'achat de {} par an-patient".format(self.drug_name)
        )
        return ax

    def _strategy_3_ticks(self, ax):
        x_tickslabels = [self.special_boxes_mapping[int(tick.get_text())]
                         for tick in ax.get_xticklabels()]
        ax.set_xticklabels(x_tickslabels)
        ax.set_xlabel("Nombre d'achat sur une annee pour un patient")

        ax.set_title(
            "Distribution de nombre d'achat de {} par an-patient".format(self.drug_name)
        )
        return ax

    @property
    def DRUG_PURCHASE_BUCKET_COLUMN(self) -> str:
        return "box_bucket"

    @property
    def DRUG_PURCHASE_BUCKET_SPECIAL_COLUMN(self) -> str:
        return "specialBucket"

    @property
    def boxes_mapping(self):
        boxes_list = range(0, 1000, 6)
        buckets = zip(boxes_list[:-1], boxes_list[1:])
        string_maps = {i: "[{}, {}[".format(bucket[0], bucket[1]) for (i, bucket) in
                       enumerate(buckets)}
        return string_maps

    @property
    def special_boxes_mapping(self):
        return {i: text for (i, text) in
                enumerate(["1 Achat", "2 Achats", "3 à 12 Achats",
                           "Plus de 12 Achats"])}

    def count_box_to_special_box_buckets(self, number_boxes):
        if number_boxes == 1:
            return 0
        if number_boxes == 2:
            return 1
        if 2 < number_boxes < 13:
            return 2
        else:
            return 3


class Antihypertenseurs(PharmaDrugsStrategy):

    @property
    def drug_name(self) -> str:
        return "Antihypertenseurs"


class Antidepresseurs(PharmaDrugsStrategy):
    @property
    def drug_name(self) -> str:
        return "Antidepresseurs"


class Hypnotiques(PharmaDrugsStrategy):
    @property
    def drug_name(self) -> str:
        return "Hypnotiques"


class Neuroleptiques(PharmaDrugsStrategy):
    @property
    def drug_name(self) -> str:
        return "Neuroleptiques"


def get_pharma_strategies() -> List[PharmaDrugsStrategy]:
    return [Antidepresseurs(), Antihypertenseurs(), Hypnotiques(), Neuroleptiques()]
