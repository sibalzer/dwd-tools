#!/usr/bin/env python3

import csv
from glob import glob
from io import TextIOWrapper
from os.path import join
from zipfile import ZipFile


print("merge dwd_stations_metadata")

with open('dwd_stations_metadata.csv', 'w', newline='') as export:
    fieldnames = [
        'stationid',
        'hoehe',
        'latitude',
        'longitude',
        'von',
        'bis',
        'stationsname'
    ]

    writer = csv.DictWriter(export, fieldnames=fieldnames)
    writer.writeheader()

    for file in glob("crawler/*.zip"):

        print(f"merging {file}")

        zipfile = ZipFile(file, 'r')
        namelist = zipfile.namelist()

        # get relevant meta data
        station_geographie_csv = zipfile.open(
            next(x for x in namelist if "Metadaten_Geographie_" in x))
        csv_reader = csv.DictReader(TextIOWrapper(
            station_geographie_csv, 'latin-1'), delimiter=';')
        station_list = []
        for row in csv_reader:

            writer.writerow({
                'stationid': row["Stations_id"].strip(),
                'hoehe': row["Stationshoehe"].strip(),
                'latitude': row["Geogr.Breite"].strip(),
                'longitude': row["Geogr.Laenge"].strip(),
                'von': row["von_datum"].strip(),
                'bis': row["bis_datum"].strip(),
                'stationsname': row["Stationsname"].strip(),
            })

print("finished press enter to exit...")
input()
