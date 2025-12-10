#!/usr/bin/python3

import argparse
import csv
from datetime import datetime
from decimal import Decimal
import sqlite3


class Lettura:
    def __init__(self, data_lettura: datetime, lettura: Decimal) -> None:
        self.data_lettura = data_lettura
        self.lettura = lettura

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
        super().__init__(
            datetime.strptime(kwargs["DATA LETTURA"], "%Y-%m-%d"),
            lettura=Decimal(LETTURA.lstrip("0")),
        )

    def __repr__(self) -> str:
        return f"{self.data_lettura} - {self.lettura} m³"

    def __str__(self) -> str:
        return f"Lettura Gas {self.__repr__()}"


class LetturaLuce(Lettura):
    FASCE = 6

    def __init__(self, data_lettura: str, **kwargs) -> None:
        super().__init__(
            datetime.strptime(data_lettura, "%d/%m/%Y"),
            Decimal(),
        )
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
    state_metadata_id, statistics_metadata_id = get_metadata_ids(
        id=f"sensor.{sensor_name}"
    )
    state = get_state(state_metadata_id=state_metadata_id)
    print(f"Importing statistics to sensor '{sensor_name}' (current state: {state}).")
    letture.sort()
    for i in range(len(letture)):
        if letture[i].lettura > state:
            try:
                max_date = letture[i + 1].data_lettura
            except IndexError:
                max_date = datetime.now()
            data = {
                "state": float(letture[i].lettura),
                "state_metadata_id": state_metadata_id,
                "statistics_metadata_id": statistics_metadata_id,
                "min_ts": letture[i].data_lettura.timestamp(),
                "max_ts": max_date.timestamp(),
            }
            update_states(**data)
            update_statistics(**data)
            print(f"Imported {letture[i]}.")


def get_metadata_ids(id: str) -> tuple[int, int]:
    res = cur.execute("SELECT metadata_id FROM states_meta WHERE entity_id = ?", (id,))
    (state_metadata_id,) = res.fetchone()
    res = cur.execute("SELECT id FROM statistics_meta WHERE statistic_id = ?", (id,))
    (statistics_metadata_id,) = res.fetchone()
    return state_metadata_id, statistics_metadata_id


def get_state(state_metadata_id: int) -> Decimal:
    res = cur.execute(
        "SELECT state FROM states WHERE metadata_id = ? ORDER BY state_id DESC LIMIT 1;",
        (state_metadata_id,),
    )
    (state,) = res.fetchone()
    return Decimal(state)


def update_states(**kwargs) -> None:
    cur.execute(
        """
        UPDATE states
        SET state = :state
        WHERE
            last_changed_ts IS NULL AND
            states.metadata_id = :state_metadata_id AND
            last_reported_ts IS NULL AND
            last_updated_ts >= :min_ts AND
            last_updated_ts < :max_ts;
        """,
        kwargs,
    )


def update_statistics(**kwargs) -> None:
    for table in ["statistics", "statistics_short_term"]:
        cur.execute(
            f"""
            UPDATE {table}
            SET state = :state, sum = ROUND(sum + state - :state, 2)
            WHERE
                metadata_id = :statistics_metadata_id AND
                start_ts >= :min_ts AND
                start_ts < :max_ts;
            """,
            kwargs,
        )


def main(filename: str):
    letture = []
    with open(filename, "r") as f:
        reader = csv.DictReader(f, delimiter=";")
        assert reader.fieldnames
        print(f"Reading file: '{filename}'.")
        if "PDR" in reader.fieldnames:
            for row in reader:
                try:
                    letture.append(LetturaGas(**row))
                except Exception as e:
                    print("Lettura parsing error: ", e)
            import_letture(
                letture=letture,
                sensor_name="lettura_gas",
            )
        elif "pod" in reader.fieldnames:
            for row in reader:
                letture.append(LetturaLuce(**row))
            for i in [1, 2, 3]:
                for lettura in letture:
                    lettura.fascia = i
                import_letture(
                    letture=letture,
                    sensor_name=f"lettura_luce_f{i}",
                )
        else:
            exit(f"{filename} not recognized, exit.")
        print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="CSV file")
    parser.add_argument("--db", help="DB file", default="home-assistant_v2.db")
    args = parser.parse_args()
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    main(filename=args.csv)
    con.commit()
    con.close()
