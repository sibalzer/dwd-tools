#!/usr/bin/env python3

from os import mkdir
import requests
from bs4 import BeautifulSoup

# Folder to download
DWD_URL = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/"


def crawl_and_download(url: str, cwd_path: str):
    dwd_index = requests.get(url).text
    soup = BeautifulSoup(dwd_index, 'html.parser')

    links = [link.get('href') for link in soup.find_all('a')
             if link.get('href') != "../"]

    for link in links:
        if link.endswith("/"):
            try:
                mkdir(f"{cwd_path}/{link}")
            except:
                pass
            print(f"move into {url+link}")
            crawl_and_download(url+link, cwd_path+link)
        else:
            while True:
                try:
                    print(f"Downloading: {link}", end='')
                    content = requests.get(url+link, timeout=10).content
                except requests.exceptions.ConnectTimeout:
                    print(f" --- failed (timeout) try again")
                    continue
                print("")
                break
            with open(f"{cwd_path}/{link}", "wb") as file:
                file.write(content)


if __name__ == "__main__":
    try:
        mkdir("crawler")
    except FileExistsError:
        pass

    crawl_and_download(DWD_URL, "crawler/")
