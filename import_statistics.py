#!/usr/bin/python3

import argparse
import csv
from datetime import datetime
from decimal import Decimal
import sqlite3


class Lettura:
    def __init__(self, data_lettura: datetime) -> None:
        self.data_lettura = data_lettura

    @property
    def lettura(self) -> Decimal:
        return NotImplemented

    @property
    def timestamp(self) -> float:
        return self.data_lettura.timestamp()

    def __float__(self) -> float:
        return float(self.lettura)

    def __lt__(self, value) -> bool:
        if isinstance(value, Lettura):
            return self.data_lettura < value.data_lettura
        return NotImplemented

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Lettura):
            return self.data_lettura == value.data_lettura
        return NotImplemented


class LetturaGas(Lettura):
    def __init__(self, LETTURA, **kwargs) -> None:
        super().__init__(datetime.strptime(kwargs["DATA LETTURA"], "%Y-%m-%d"))
        self._lettura = Decimal(LETTURA.lstrip("0"))

    @property
    def lettura(self):
        return self._lettura

    def __repr__(self) -> str:
        return f"{self.data_lettura} - {self.lettura} m³"

    def __str__(self) -> str:
        return f"Lettura Gas {self.__repr__()}"


class LetturaLuce(Lettura):
    FASCE = 6

    def __init__(self, data_lettura: str, **kwargs) -> None:
        super().__init__(datetime.strptime(data_lettura, "%d/%m/%Y"))
        for i in range(1, self.FASCE + 1):
            setattr(self, f"lettura_f{i}", Decimal(kwargs[f"lettura_f{i}"]))
        self.fascia = 0  # the 'fascia' that 'lettura' property returns

    def __repr__(self) -> str:
        return "{} - {}".format(
            self.data_lettura,
            ", ".join(
                [
                    f"F{i} {self._lettura(i)} kWh"
                    for i in range(1, self.FASCE + 1)
                    if self._lettura(i) != 0
                ]
            ),
        )

    def __str__(self) -> str:
        return f"Lettura Luce F{self.fascia} {self.data_lettura} - {self.lettura} kWh"

    def _lettura(self, fascia) -> Decimal:
        return getattr(self, f"lettura_f{fascia}")

    @property
    def lettura(self) -> Decimal:
        return self._lettura(self.fascia)


def import_letture(letture: list[Lettura], sensor_name: str) -> None:
    id = f"sensor.{sensor_name}"
    print(f"Importing statistics to '{id}'.")
    state_metadata_id = get_state_metadata_id(id)
    statistics_metadata_id = get_statistics_metadata_id(id)
    for lettura in sorted(letture):
        data = {
            "state": float(lettura),
            "state_metadata_id": state_metadata_id,
            "statistics_metadata_id": statistics_metadata_id,
            "min_ts": lettura.timestamp,
        }
        update_states(**data)
        update_statistics(**data)
        print(f"Imported {lettura}.")


def get_state_metadata_id(entity_id: str) -> int:
    res = cur.execute(
        "SELECT metadata_id FROM states_meta WHERE entity_id = ?", (entity_id,)
    )
    return res.fetchone()[0]


def get_statistics_metadata_id(statistic_id: str) -> int:
    res = cur.execute(
        "SELECT id FROM statistics_meta WHERE statistic_id = ?", (statistic_id,)
    )
    return res.fetchone()[0]


def update_states(**kwargs) -> None:
    cur.execute(
        """
        UPDATE states
        SET state = :state
        WHERE
            lt(state, :state) AND
            states.metadata_id = :state_metadata_id AND
            last_updated_ts >= :min_ts;
        """,
        kwargs,
    )


def update_statistics(**kwargs) -> None:
    for table in ["statistics", "statistics_short_term"]:
        cur.execute(
            f"""
            UPDATE {table}
            SET state = :state, sum = ROUND(sum + state - :state, 3)
            WHERE
                state < :state AND
                metadata_id = :statistics_metadata_id AND
                start_ts >= :min_ts;
            """,
            kwargs,
        )


def main(filename: str):
    with open(filename, "r") as f:
        reader = csv.DictReader(f, delimiter=";")
        assert reader.fieldnames
        print(f"Reading file: '{filename}'.")
        letture = []
        if "PDR" in reader.fieldnames:
            L = LetturaGas
        elif "pod" in reader.fieldnames:
            L = LetturaLuce
        else:
            exit(f"{filename} not recognized, exit.")
        for row in reader:
            try:
                letture.append(L(**row))
            except Exception as e:
                print("Lettura parsing error: ", e)

    if L == LetturaGas:
        import_letture(
            letture=letture,
            sensor_name="lettura_gas",
        )
    elif L == LetturaLuce:
        for i in [1, 2, 3]:
            for lettura in letture:
                lettura.fascia = i
            import_letture(
                letture=letture,
                sensor_name=f"lettura_luce_f{i}",
            )
    print("Done.")


def lt(arg1, arg2) -> bool:
    try:
        return float(arg1) < float(arg2)
    except ValueError:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="CSV file")
    parser.add_argument("--db", help="DB file", default="home-assistant_v2.db")
    args = parser.parse_args()
    con = sqlite3.connect(args.db)
    con.create_function("lt", 2, lt)
    cur = con.cursor()
    main(filename=args.csv)
    con.commit()
    con.close()
