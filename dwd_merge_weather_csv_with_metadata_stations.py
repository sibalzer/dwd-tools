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


with open('dwd_weather_with_metadata_stations.csv', 'w',  newline='') as export:
    fieldnames = [
        'stationid',
        'stationname',
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
        'temperatur_in_hoehe_5cm_min'
    ]

    writer = csv.DictWriter(export, fieldnames=fieldnames)
    writer.writeheader()

    for file in glob("crawler/*.zip"):

        print(f"merging {file}")

        zipfile = ZipFile(file, 'r')
        namelist = zipfile.namelist()

        # get relevant meta data
        station_geographie_csv = zipfile.open(
            next(x for x in namelist if x.startswith("Metadaten_Geographie_")))
        csv_reader = csv.DictReader(TextIOWrapper(
            station_geographie_csv, 'latin-1'), delimiter=';')
        station_list = []
        for row in csv_reader:
            station = Station()
            station.name = row["Stationsname"]
            station.latitude = row["Geogr.Breite"]
            station.longitude = row["Geogr.Laenge"]
            station.start = datetime.strptime(row["von_datum"], r"%Y%m%d")
            if row["bis_datum"] != "        ":
                station.end = datetime.strptime(row["bis_datum"], r"%Y%m%d")
            else:
                station.end = datetime(9999,month=12,day=31)
            station.height = row["Stationshoehe"]

            station_list.append(station)

        data_csv = zipfile.open(
            next(x for x in namelist if "produkt_klima_tag" in x))

        csv_reader = csv.DictReader(
            TextIOWrapper(data_csv, 'latin-1'), delimiter=';')

        for row in csv_reader:

            date = datetime.strptime(row["MESS_DATUM"], r"%Y%m%d")

            station = next(
                item for item in station_list if item.start <= date and date <= item.end)

            RSKF_dict: dict = dict()

            RSKF_dict[0] = "kein Niederschlag (konventionelle oder automatische Messung), entspricht WMO Code-Zahl 10"
            RSKF_dict[1] = "nur Regen (in historischen Daten vor 1979)"
            RSKF_dict[2] = ""
            RSKF_dict[3] = ""
            RSKF_dict[4] = "Form nicht bekannt, obwohl Niederschlag gemeldet"
            RSKF_dict[5] = ""
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
                'temperatur_in_hoehe_5cm_min': row[" TGK"].strip()
            })
