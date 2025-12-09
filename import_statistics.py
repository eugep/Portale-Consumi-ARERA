import argparse
import csv
from datetime import datetime
from decimal import Decimal
import sqlite3


def gas(filename, state_metadata_id, statistic_metadata_id: int):
    DATE_KEY = "DATA LETTURA"
    DATE_FORMAT = "%Y-%m-%d"
    state, sum = get_latest_state_and_sum(metadata_id=statistic_metadata_id)

    with open(filename, "r") as f:
        lines = list(csv.DictReader(f, delimiter=";"))
        lines.sort(key=lambda l: l.get(DATE_KEY, ""))
        for i in range(len(lines)):
            try:
                from_date = datetime.strptime(lines[i][DATE_KEY], DATE_FORMAT)
                lettura = Decimal(lines[i]["LETTURA"].lstrip("0"))
            except:
                continue

            if lettura <= state:
                continue
            sum += lettura - state
            state = lettura
            try:
                to_date = datetime.strptime(lines[i + 1][DATE_KEY], DATE_FORMAT)
            except IndexError:
                to_date = datetime.now()

            run_sql(
                state=lettura,
                sum=sum,
                statistic_metadata_id=statistic_metadata_id,
                state_metadata_id=state_metadata_id,
                from_date=from_date,
                to_date=to_date,
            )


def luce_giornaliera(
    filename,
    state_meta_ids: list[int],
    statistic_meta_ids: list[int],
    update_since: datetime = datetime.now(),
):
    DATE_KEY = "data_lettura"
    DATE_FORMAT = "%d/%m/%Y"
    FASCE = [1, 2, 3]

    with open(filename, "r") as f:
        lines = list(csv.DictReader(f, delimiter=";"))
        lines.sort(key=lambda l: datetime.strptime(l[DATE_KEY], DATE_FORMAT))
        for fascia, statistic_metadata_id, state_metadata_id in zip(
            FASCE, statistic_meta_ids, state_meta_ids
        ):
            state, sum = get_latest_state_and_sum(
                metadata_id=statistic_metadata_id, date_limit=update_since
            )

            for i in range(len(lines)):
                from_date = datetime.strptime(lines[i][DATE_KEY], DATE_FORMAT)
                lettura = Decimal(lines[i][f"lettura_f{fascia}"])

                if from_date < update_since:
                    continue
                sum += lettura - state
                state = lettura
                try:
                    to_date = datetime.strptime(lines[i + 1][DATE_KEY], DATE_FORMAT)
                except IndexError:
                    to_date = datetime.now()
                run_sql(
                    state=lettura,
                    sum=sum,
                    statistic_metadata_id=statistic_metadata_id,
                    state_metadata_id=state_metadata_id,
                    from_date=from_date,
                    to_date=to_date,
                )


def update_state(
    state_metadata_id: int,
    state: Decimal,
    from_date: datetime,
    to_date: datetime = datetime.now(),
):
    cur.execute(
        """
        UPDATE states 
        SET state = {} 
        WHERE 
            last_changed_ts IS NULL AND 
            states.metadata_id = {} AND 
            last_reported_ts IS NULL AND
            last_updated_ts>={} AND 
            last_updated_ts<{};
        """.format(
            state, state_metadata_id, from_date.timestamp(), to_date.timestamp()
        )
    )


def run_sql(
    state: Decimal,
    sum: Decimal,
    statistic_metadata_id: int,
    state_metadata_id: int,
    from_date: datetime,
    to_date: datetime = datetime.now(),
):
    for table in ["statistics", "statistics_short_term"]:
        cur.execute(
            "UPDATE {} SET state={}, sum={} WHERE metadata_id={} AND start_ts>={} AND start_ts<{};".format(
                table,
                state,
                sum,
                statistic_metadata_id,
                from_date.timestamp(),
                to_date.timestamp(),
            )
        )
    update_state(
        state_metadata_id=state_metadata_id,
        state=state,
        from_date=from_date,
        to_date=to_date,
    )


def get_latest_state_and_sum(
    metadata_id: int, date_limit: datetime = datetime.now()
) -> tuple[Decimal, Decimal]:
    res = cur.execute(
        'SELECT state, "sum" FROM statistics WHERE metadata_id = {} AND start_ts < {} ORDER BY id DESC LIMIT 1;'.format(
            metadata_id, date_limit.timestamp()
        )
    )
    state, sum = res.fetchone()
    return Decimal(str(state)), Decimal(str(sum))


def process_letture(
    letture: list[tuple[datetime, Decimal]],
    statistic_metadata_id: int,
    state_metadata_id: int,
    state: Decimal = Decimal(0),
    sum: Decimal = Decimal(0),
):
    for i in range(len(letture)):
        if letture[i][1] <= state:
            continue
        sum += letture[i][1] - state
        state = letture[i][1]
        from_date = letture[i][0]
        try:
            to_date = letture[i + 1][0]
        except IndexError:
            to_date = datetime.now()
        run_sql(
            state=state,
            sum=sum,
            statistic_metadata_id=statistic_metadata_id,
            state_metadata_id=state_metadata_id,
            from_date=from_date,
            to_date=to_date,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="CSV file")
    parser.add_argument(
        "--db",
        help="DB file",
        default="/home/pi/homeassistant/config/home-assistant_v2.db",
    )
    args = parser.parse_args()

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    with open(args.csv, "r") as f:
        headers = f.readline()
        if headers.startswith("PDR"):
            print(f"Importing GAS statistics from '{args.csv}'")
            gas(args.csv, state_metadata_id=259, statistic_metadata_id=18)
        elif headers.startswith("pod"):
            print(f"Importing ENERGY statistics from '{args.csv}'")
            luce_giornaliera(
                args.csv,
                state_meta_ids=[256, 257, 258],
                statistic_meta_ids=[11, 12, 13],
                update_since=datetime(2025, 10, 9),
            )
        else:
            print(f"{args.csv} not recognized, exit.")

    con.commit()
    con.close()
