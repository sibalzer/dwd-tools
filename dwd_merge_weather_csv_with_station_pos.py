#!/usr/bin/env python3

import csv
from datetime import datetime, timedelta
from glob import glob
from io import TextIOWrapper
from os import stat
from os.path import join
from zipfile import ZipFile


class Station:
    name: str
    start: datetime
    end: datetime
    latitude: float
    longitude: float
    height: float


print("merge dwd_weather_with_station_pos")

with open('dwd_weather_with_station_pos.csv', 'w',  newline='') as export:
    fieldnames = [
        'stationid',
        'date',
        'latitude',
        'longitude',
        'height',
        'quality_wind',
        'wind_max',
        'wind_mean',
        'quality_rest',
        'niederschlagshöhe',
        'niederschlagsform_id',
        'niederschlagsform_description',
        'sonnenscheindauer',
        'schneehoehe',
        'bedeckungsgrad_mean',
        'dampfdruck_mean',
        'luftdruck_mean',
        'temperatur_mean',
        'relativen_feuchte_mean',
        'temperatur_in_hoehe_2m_max',
        'temperatur_in_hoehe_2m_min',
        'temperatur_in_hoehe_5cm_min',
        'faulty_station_metadata'
    ]

    writer = csv.DictWriter(export, fieldnames=fieldnames)
    writer.writeheader()

    for file in glob("crawler/*.zip"):
        err_str = ""

        print(f"merging {file}")

        zipfile = ZipFile(file, 'r')
        namelist = zipfile.namelist()

        # get relevant meta data
        station_geographie_csv = zipfile.open(
            next(x for x in namelist if x.startswith("Metadaten_Geographie_")))
        csv_reader = csv.DictReader(TextIOWrapper(
            station_geographie_csv, 'latin-1'), delimiter=';')
        station_list = []
        err_str += "  Stations in this dataset:\n"
        for row in csv_reader:
            station = Station()
            station.name = row["Stationsname"]
            station.latitude = row["Geogr.Breite"]
            station.longitude = row["Geogr.Laenge"]
            station.start = datetime.strptime(row["von_datum"], r"%Y%m%d")
            if row["bis_datum"] != "        ":
                station.end = datetime.strptime(row["bis_datum"], r"%Y%m%d")
            else:
                station.end = datetime(9999, month=12, day=31)
            station.height = row["Stationshoehe"]
            err_str += f"  Name: {station.name} {station.start.strftime(r'%d.%m.%Y')}-{station.end.strftime(r'%d.%m.%Y')} | ({station.latitude}, {station.longitude}) {station.height}\n"

            station_list.append(station)

        data_csv = zipfile.open(
            next(x for x in namelist if "produkt_klima_tag" in x))

        csv_reader = csv.DictReader(
            TextIOWrapper(data_csv, 'latin-1'), delimiter=';')

        err_str += f"  Parsing rows...\n"

        row_counter = 0
        for row in csv_reader:
            row_counter += 1
            faulty = False
            date = datetime.strptime(row["MESS_DATUM"], r"%Y%m%d")

            try:
                station = next(
                    s for s in station_list if s.start <= date and date <= s.end)
            except StopIteration:
                err_str += f"  Error - No Station found for time {date.strftime(r'%d.%m.%Y')} (row {row_counter}) added with faulty flag\n"
                unknown = Station()
                unknown.latitude = "-999"
                unknown.longitude = "-999"
                unknown.height = "-999"
                unknown.name = "Unknown"
                faulty = True
                print(err_str, end='')
                err_str = ""

            RSKF_dict: dict = dict()

            RSKF_dict[0] = "kein Niederschlag (konventionelle oder automatische Messung), entspricht WMO Code-Zahl 10"
            RSKF_dict[1] = "nur Regen (in historischen Daten vor 1979)"
            RSKF_dict[2] = "-999"
            RSKF_dict[3] = "-999"
            RSKF_dict[4] = "Form nicht bekannt, obwohl Niederschlag gemeldet"
            RSKF_dict[5] = "-999"
            RSKF_dict[6] = "nur Regen; flüssiger Niederschlag bei automatischen Stationen, entspricht WMO Code-Zahl 11"
            RSKF_dict[7] = "nur Schnee; fester Niederschlag bei automatischen Stationen, entspricht WMO Code-Zahl 12"
            RSKF_dict[8] = "Regen und Schnee (und/oder Schneeregen); flüssiger und fester Niederschlag bei automatischen Stationen, entspricht WMO Code-Zahl 13"
            RSKF_dict[9] = "fehlender Wert oder Niederschlagsform nicht feststellbar bei automatischer Messung, entspricht WMO Code-Zahl 15"
            RSKF_dict[-999] = "-999"

            writer.writerow({
                'stationid': row["STATIONS_ID"].strip(),
                'date': row["MESS_DATUM"].strip(),
                'latitude': station.latitude.strip(),
                'longitude': station.longitude.strip(),
                'height': station.height.strip(),
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
                'temperatur_in_hoehe_5cm_min': row[" TGK"].strip(),
                'faulty_station_metadata': faulty
            })

print("finished press enter to exit...")
input()
