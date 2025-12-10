# Import energy data from Portale Consumi to Home Assistant

Import csv file downloaded from the ARERA [Portale Consumi](https://www.consumienergia.it/) to Home Assistant [Energy Management](https://www.home-assistant.io/docs/energy/).

The script provided read a csv file containing the gas or energy measurements and import the data as [long- and short-term statistics](https://data.home-assistant.io/docs/statistics) into [Home Assistant Database](https://www.home-assistant.io/docs/backend/database/).

## Download csv measurements file

1. Login to [Portale Consumi](https://www.consumienergia.it/) with your Identity Provider.
2. Choose your energy utility (**gas** or **light**).
3. Download the csv file.

## Home Assistant sensors

Create one [template sensor](https://www.home-assistant.io/integrations/template#sensor) for each energy meter.

For instance:
- sensor.lettura_gas
- sensor.lettura_luce_f1
- sensor.lettura_luce_f2
- sensor.lettura_luce_f3


## Run the script

1. `Stop` your Home Assistant instance.
2. Run the script with the following command _(replace csv and db filepaths)_ .
```
./import_statistics.py <csv_filepath> --db <db_filepath>
```
3. `Restart` your Home Assistant instance.
