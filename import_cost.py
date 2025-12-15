#!/usr/bin/python3

import argparse
from datetime import datetime
from decimal import Decimal
import sqlite3
from enum import Enum

from a2a_scraper import A2A, F, Mese


def get_statistics_metadata_id(statistic_id: str) -> int:
    res = cur.execute(
        "SELECT id FROM statistics_meta WHERE statistic_id = ?", (statistic_id,)
    )
    return res.fetchone()[0]


class Indice(Enum):
    PUN = "PUN"
    PSV = "PSV"


def import_cost(indice:Indice, sensor_name: str, f:F|None=None) -> None:
    id = f"sensor.{sensor_name}"
    lettura_metadata_id = get_statistics_metadata_id(id)
    cost_metadata_id = get_statistics_metadata_id(f"{id}_cost")

    res = cur.execute("SELECT start_ts, state FROM statistics WHERE metadata_id = ?;", (lettura_metadata_id,))
    lettura_precedente = None
    total_cost = Decimal('0')
    for start_ts, state in res.fetchall():
        lettura = Decimal(str(state))
        if lettura_precedente is None:
            lettura_precedente = lettura
            continue

        # Cost
        start_date = datetime.fromtimestamp(start_ts)
        mese = Mese(start_date.month)
        anno = start_date.year
        price = 0
        if indice == Indice.PSV:
            if anno == 2025 and mese.value >= 9:
                price = Decimal("0.505")  # fixed price
            else:
                price = A2A.get_psv(mese=mese, anno=anno)
                if price is None:
                    continue
                price = price.PSV
        elif indice == Indice.PUN:
            if anno == 2025 and mese.value >= 9:
                price = Decimal("0.122")  # fixed price
            else:
                price = A2A.get_pun(mese=mese, anno=anno)
                if price is None:
                    continue
                assert f is not None
                price = getattr(price, f.name)
        assert price > 0

        total_cost += (lettura - lettura_precedente) * price

        data = {
            "total_cost": round(float(total_cost), 2),
            "statistics_metadata_id": cost_metadata_id,
            "ts": start_ts,
        }
        cur.execute(
            f"""
            UPDATE statistics
            SET state = :total_cost, sum = :total_cost
            WHERE
                metadata_id = :statistics_metadata_id AND
                start_ts = :ts;
            """,
            data,
        )

        lettura_precedente = lettura


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("indice", choices=["PSV", "PUN"])
    parser.add_argument("--db", help="DB file", default="home-assistant_v2.db")
    args = parser.parse_args()
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    if args.indice == "PSV":
        import_cost(indice=Indice.PSV, sensor_name="lettura_gas")
    elif args.indice == "PUN":
        for f in [F.F1, F.F2, F.F3]:
            import_cost(indice=Indice.PUN, f=f, sensor_name=f"lettura_luce_{f.name.lower()}")
    con.commit()
    con.close()
