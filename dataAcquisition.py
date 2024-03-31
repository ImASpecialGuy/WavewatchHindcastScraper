from bs4 import BeautifulSoup
import requests
import os
from pathlib import Path
import threading
import multiprocessing
import argparse


def printDates(start_month, number_of_months, args):
    def download_file(file_url, download_folder):
        response = requests.get(file_url, stream=True)
        response.raise_for_status()  # Raise an exception for bad response codes

        with open(str(download_folder / file_url.split('/')[-1]), "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                # if chunk:  # Filter out keep-alive chunks
                file.write(chunk)

    base_url = "https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/"
    base_path = Path(args.path)
    for month in range(start_month, start_month + number_of_months):
        # this is painful because January is month 1 and not month 0
        date_string = str((month - 1) // 12) + "{:02d}".format((month - 1) % 12 + 1)
        gribs_folder = base_path / Path(f"{date_string}/gribs")
        gribs_folder.mkdir(parents=True, exist_ok=True)
        for region in args.regions:
            for feature in args.features:
                gribs_url = f"{base_url}{date_string}/gribs/multi_reanal.{regions_dictionary[region]}.{features_dictionary[feature]}.{date_string}.grb2"
                download_file(gribs_url, gribs_folder)


def main():
    # TODO: PROGRESS BAR
    def clamp_month(month):
        return max(1, min(month, 12))

    def clamp_year(year):
        return max(1979, min(year, 2009))

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start-date", type=str, default="01-1979",
                        help="Inclusive start date in format 'MM-YYYY' (min: 01-1979; max: 12-2009; default: 01-1979)")
    parser.add_argument("-e", "--end-date", type=str, default="12-2009",
                        help="Inclusive end date in format 'MM-YYYY' (min: 01-1979; max: 12-2009; default: 12-2009)")
    parser.add_argument("-p", "--path", type=str, default="data",
                        help="Path relative to this script where data will be downloaded to (default: data")
    parser.add_argument(
        "-r", "--regions",
        nargs="+",
        help="Space-separated list of regions to download (default: ALL)."
             "\n Available regions (alphabetical order): alaska, alaska-coastal, australia, "
             "australia-coastal, east-us, east-us-coastal, global, mediterranean, north-sea, "
             "north-sea-coastal, nw-indian-ocean, pacific-islands, west-us-coastal, west-us",
        choices=regions_dictionary.keys(),
        default=regions_dictionary.keys(),
    )
    parser.add_argument(
        "-f", "--features",
        nargs="+",
        help="Space-separated list of features to download (default: ALL)."
             "\n Available features: wave-height, wind-speeds, wave-period, wave-direction",
        choices=features_dictionary.keys(),
        default=features_dictionary.keys(),
    )
    args = parser.parse_args()
    start_month, start_year = map(int, args.start_date.split("-"))
    end_month, end_year = map(int, args.end_date.split("-"))
    base_path = Path(args.path)
    base_path.mkdir(parents=True, exist_ok=True)

    # Catch invalid dates by clamping them to the min and max values
    start_month, end_month = map(clamp_month, (start_month, end_month))
    start_year, end_year = map(clamp_year, (start_year, end_year))

    # Add one because if end_year == start_year and end_month == start_month, we should do 1 month
    number_of_months = end_year * 12 + end_month - start_year * 12 - start_month + 1
    # Putting everything in start_month is easier for workload distribution amongst threads
    start_month += start_year * 12

    # Divide work amongst threads and call printDates on those threads
    num_cores = multiprocessing.cpu_count()
    months_per_thread = number_of_months // num_cores
    threads_workload = [months_per_thread for i in range(num_cores)]
    remaining_months = number_of_months % num_cores
    threads = []

    for i in range(len(threads_workload)):
        # evenly divide the remaining months
        if remaining_months > 0:
            threads_workload[i] += 1
            remaining_months -= 1

        thread = threading.Thread(target=printDates, args=(start_month, threads_workload[i], args,))
        thread.start()
        threads.append(thread)
        start_month += threads_workload[i]

    # Wait for the threads to finish
    for thread in threads:
        thread.join()


regions_dictionary = {"alaska": "ak_4m",
                      "alaska-coastal": "ak_10m",
                      "east-us-coastal": "ecg_4m",
                      "east-us": "ecg_10m",
                      "global": "glo_30m",
                      "mediterranean": "med_10m",
                      "north-sea-coastal": "nsb_4m",
                      "north-sea": "nsb_10m",
                      "nw-indian-ocean": "nwio_10m",
                      "australia-coastal": "oz_4m",
                      "australia": "oz_10m",
                      "pacific-islands": "pi_10m",
                      "west-us-coastal": "wc_4m",
                      "west-us": "wc_10m"}
features_dictionary = {"wave-height": "hs",
                       "wind-speeds": "wind",
                       "wave-period": "tp",
                       "wave-direction": "dp"}

if __name__ == "__main__":
    main()
