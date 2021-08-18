#!/usr/bin/env python3

import csv
from datetime import datetime
from glob import glob
from io import TextIOWrapper
from os.path import join
from zipfile import ZipFile


print("merge dwd_weather_csv")

with open('dwd_weather.csv', 'w', newline='') as export:

    fieldnames = [
        'stationid',
        'date',
        'wind_max',
        'wind_mean',
        'niederschlagshöhe',
        'niederschlagsform_id',
        'sonnenscheindauer',
        'schneehoehe',
        'bedeckungsgrad_mean',
        'dampfdruck_mean',
        'luftdruck_mean',
        'temperatur_mean',
        'relativen_feuchte_mean',
        'temperatur_in_hoehe_2m_max',
        'temperatur_in_hoehe_2m_min',
        'temperatur_in_hoehe_5cm_min'
    ]

    writer = csv.DictWriter(export, fieldnames=fieldnames)
    writer.writeheader()

    for file in glob("crawler/*.zip"):

        print(f"merging {file}")

        zipfile = ZipFile(file, 'r')
        namelist = zipfile.namelist()

        data_csv = zipfile.open(
            next(x for x in namelist if x.startswith("produkt_klima_tag")))

        csv_reader = csv.DictReader(
            TextIOWrapper(data_csv, 'latin-1'), delimiter=';')

        for row in csv_reader:

            date = datetime.strptime(row["MESS_DATUM"], r"%Y%m%d")

            RSKF_dict: dict = dict()

            RSKF_dict[0] = "kein Niederschlag (konventionelle oder automatische Messung), entspricht WMO Code-Zahl 10"
            RSKF_dict[1] = "nur Regen (in historischen Daten vor 1979)"
            RSKF_dict[2] = "Unknown"
            RSKF_dict[3] = "Unknown"
            RSKF_dict[4] = "Form nicht bekannt, obwohl Niederschlag gemeldet"
            RSKF_dict[5] = "Unknown"
            RSKF_dict[6] = "nur Regen; flüssiger Niederschlag bei automatischen Stationen, entspricht WMO Code-Zahl 11"
            RSKF_dict[7] = "nur Schnee; fester Niederschlag bei automatischen Stationen, entspricht WMO Code-Zahl 12"
            RSKF_dict[8] = "Regen und Schnee (und/oder Schneeregen); flüssiger und fester Niederschlag bei automatischen Stationen, entspricht WMO Code-Zahl 13"
            RSKF_dict[9] = "fehlender Wert oder Niederschlagsform nicht feststellbar bei automatischer Messung, entspricht WMO Code-Zahl 15"
            RSKF_dict[-999] = "-999"

            writer.writerow({
                'stationid': row["STATIONS_ID"].strip(),
                'date': row["MESS_DATUM"].strip(),
                'wind_max': row["  FX"].strip(),
                'wind_mean': row["  FM"].strip(),
                'niederschlagshöhe': row[" RSK"].strip(),
                'niederschlagsform_id': row["RSKF"].strip(),
                'sonnenscheindauer': row[" SDK"].strip(),
                'schneehoehe': row["SHK_TAG"].strip(),
                'bedeckungsgrad_mean': row["  NM"].strip(),
                'dampfdruck_mean': row[" VPM"].strip(),
                'luftdruck_mean': row["  PM"].strip(),
                'temperatur_mean': row[" TMK"].strip(),
                'relativen_feuchte_mean': row[" UPM"].strip(),
                'temperatur_in_hoehe_2m_max': row[" TXK"].strip(),
                'temperatur_in_hoehe_2m_min': row[" TNK"].strip(),
                'temperatur_in_hoehe_5cm_min': row[" TGK"].strip()
            })

print("finished press enter to exit...")
input()
