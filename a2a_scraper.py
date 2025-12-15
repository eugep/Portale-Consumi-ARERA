from decimal import Decimal
from enum import IntEnum

from lxml import etree
import requests


class Mese(IntEnum):
    Gennaio = 1
    Febbraio = 2
    Marzo = 3
    Aprile = 4
    Maggio = 5
    Giugno = 6
    Luglio = 7
    Agosto = 8
    Settembre = 9
    Ottobre = 10
    Novembre = 11
    Dicembre = 12


class Indice:
    def __init__(self, tr) -> None:
        mese, anno = tr[0].text.split(" ")
        self.mese = Mese[mese]
        self.anno = int(anno)

    def __repr__(self) -> str:
        return f"{self.mese.name} {self.anno}"


class PSV(Indice):
    def __init__(self, tr) -> None:
        super().__init__(tr)
        self.PSV = Decimal(tr[1].text.replace(",", "."))

    def __repr__(self) -> str:
        return f"{super().__repr__()} - PSV {self.PSV} €/Smc"

    def __float__(self) -> float:
        return float(self.PSV)


class F(IntEnum):
    MONORARIO = 1
    F1 = 2
    F2 = 3
    F3 = 4
    F23 = 5


class PUN(Indice):
    def __init__(self, tr) -> None:
        super().__init__(tr)
        for i in range(1, 1 + len(F)):
            setattr(self, F(i).name, Decimal(tr[i].text.replace(",", ".")))

    def __repr__(self) -> str:
        return f"{super().__repr__()} - " + ", ".join(
            [f"{F(1).name} {getattr(self, F(i).name)} €/kWh" for i in range(1, 4)]
        )


class A2A_scraper:
    def __init__(self) -> None:
        self.session = requests.Session()
        self._pun = list(self._parse_pun())
        self._psv = list(self._parse_psv())

    def _parse_psv(self):
        url = "https://www.a2a.it/assistenza/tutela-cliente/indici/indice-psv"
        response = self.session.get(url)
        response.raise_for_status()
        tree = etree.HTML(response.content)
        for tr in tree.xpath(
            "/html/body/div/div[1]/main/div[2]/div/div[3]/div/div/div/div/table/tbody/tr"
        ):
            yield PSV(tr)

    def _parse_pun(self):
        url = "https://www.a2a.it/assistenza/tutela-cliente/indici/indice-pun"
        response = self.session.get(url)
        response.raise_for_status()
        tree = etree.HTML(response.text)
        for tr in tree.xpath(
            "/html/body/div/div[1]/main/div[2]/div/div[3]/div/div[5]/div/div/div[2]/div/div/div/table/tbody/tr"
        ):
            yield PUN(tr)

    def get_pun(self, mese: Mese, anno: int) -> PUN | None:
        for pun in self._pun:
            if pun.mese == mese and pun.anno == anno:
                return pun

    def get_psv(self, mese: Mese, anno: int) -> PSV | None:
        for psv in self._psv:
            if psv.mese == mese and psv.anno == anno:
                return psv


A2A = A2A_scraper()

if __name__ == "__main__":
    A2A.get_psv(mese=Mese.Ottobre, anno=2025)
    A2A.get_pun(mese=Mese.Ottobre, anno=2025)
