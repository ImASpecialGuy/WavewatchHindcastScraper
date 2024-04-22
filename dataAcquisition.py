import os.path

import requests
from pathlib import Path
import threading
import multiprocessing
import argparse
import time
from alive_progress import alive_bar


def scrape_files(start_month, number_of_months, args):
    def download_file(file_url, download_folder):
        try:
            response = requests.get(file_url, stream=True)
            response.raise_for_status()  # Raise an exception for bad response codes

            file_path = Path(download_folder) / file_url.split('/')[-1]  # Using Pathlib for safer handling

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    # If chunk:  # Filter out keep-alive chunks (if necessary)
                    file.write(chunk)
            return True

        except requests.exceptions.RequestException as e:
            print(f"Error downloading file: {e}")

        except OSError as e:
            print(f"Error writing to file: {e}")

        except Exception as e:
            print(f"An unknown error occurred: {e}")

    def download_gribs_files():
        # Prepare the folders
        gribs_folder = base_path / Path(f"{date_string}/gribs")
        gribs_folder.mkdir(parents=True, exist_ok=True)
        for region in args.regions:
            for feature in args.features:
                # Global region name differs in partition and gribs folders
                if region == 'global':
                    gribs_url = f"{base_url}{date_string}/gribs/multi_reanal.{regions_dictionary[region][0]}.{features_dictionary[feature]}.{date_string}.grb2"
                    # Only download if the file does not exist yet
                    file_name = gribs_folder / Path(f"multi_reanal.{regions_dictionary[region][0]}.{features_dictionary[feature]}.{date_string}.grb2")
                else:
                    gribs_url = f"{base_url}{date_string}/gribs/multi_reanal.{regions_dictionary[region]}.{features_dictionary[feature]}.{date_string}.grb2"
                    # Only download if the file does not exist yet
                    file_name = gribs_folder / Path(f"multi_reanal.{regions_dictionary[region]}.{features_dictionary[feature]}.{date_string}.grb2")

                if not os.path.isfile(file_name):
                    download_file(gribs_url, gribs_folder)

                # Add it to the tally
                with counter_lock:
                    global processed_files
                    processed_files += 1

    def download_partition_files():
        # prepare the folders
        partitions_folder = base_path / Path(f"{date_string}/partitions")
        partitions_folder.mkdir(parents=True, exist_ok=True)
        for region in args.regions:
            # global region name differs in partition and gribs folders
            if region == 'global':
                partitions_url = f"{base_url}{date_string}/partitions/multi_reanal.partition.{regions_dictionary[region][1]}.{date_string}.nc"
                # Only download if the file does not exist yet
                file_name = partitions_folder / Path(f"multi_reanal.partition.{regions_dictionary[region][1]}.{date_string}.nc")
            else:
                partitions_url = f"{base_url}{date_string}/partitions/multi_reanal.partition.{regions_dictionary[region]}.{date_string}.nc"
                # Only download if the file does not exist yet
                file_name = partitions_folder / Path(f"multi_reanal.partition.{regions_dictionary[region]}.{date_string}.nc")

            if not os.path.isfile(file_name):
                download_file(partitions_url, partitions_folder)

            # Add it to the tally
            with counter_lock:
                global processed_files
                processed_files += 1

    def download_buoy_files():
        # Prepare the folders
        buoy_folder = base_path / Path(f"{date_string}/points/buoys")
        virtual_folder = base_path / Path(f"{date_string}/points/virtual")
        buoy_folder.mkdir(parents=True, exist_ok=True)
        virtual_folder.mkdir(parents=True, exist_ok=True)

        # Construct the urls and download them into their respective folders
        # Create a list of tuples [(url1, folder1), (url2, folder2), ...]
        urls = [(f"{base_url}{date_string}/points/{type}/multi_reanal.{part}.{type}.{date_string}.tar.gz",
                 base_path / Path(f"{date_string}/points/{type}"))
                for part in ["buoys_part", "buoys_spec", "buoys_wmo"] for type in ["buoys", "virtual"]]
        for url, file_name in urls:
            # Only download if the file does not exist yet
            if not os.path.isfile(file_name):
                download_file(url, file_name)
            # Add it to the tally
            with counter_lock:
                global processed_files
                processed_files += 1

    base_url = "https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/"
    base_path = Path(args.path)
    for month in range(start_month, start_month + number_of_months):
        # this is painful because January is month 1 and not month 0
        date_string = str((month - 1) // 12) + "{:02d}".format((month - 1) % 12 + 1)

        # download all the data the user has specified
        if not args.no_data:
            download_gribs_files()
        if args.partition_files:
            download_partition_files()
        if args.buoy_files:
            download_buoy_files()


def main():
    def clamp_month(month):
        return max(1, min(month, 12))

    def clamp_year(year):
        return max(1979, min(year, 2009))

    def calculate_number_of_files():
        data_contribution = 0 if args.no_data else len(args.features)
        partition_contribution = 1 if args.partition_files else 0
        buoy_contribution = 6 if args.buoy_files else 0
        return number_of_months * (buoy_contribution + len(args.regions) * (data_contribution + partition_contribution))

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start-date", type=str, default="01-1979",
                        help="Inclusive start date in format 'MM-YYYY' (min: 01-1979; max: 12-2009; default: 01-1979)")
    parser.add_argument("-e", "--end-date", type=str, default="12-2009",
                        help="Inclusive end date in format 'MM-YYYY' (min: 01-1979; max: 12-2009; default: 12-2009)")
    parser.add_argument("-p", "--path", type=str, default="data",
                        help="Path relative to this script where data will be downloaded to (default: data).")
    parser.add_argument(
        "-r", "--regions",
        nargs="+",
        metavar="REGION",
        help="Space-separated list of regions to download (default: ALL)."
             "\n Available regions (alphabetical order): alaska, alaska-coastal, australia, "
             "australia-coastal, east-us, east-us-coastal, global, mediterranean, north-sea, "
             "north-sea-coastal, nw-indian-ocean, pacific-islands, west-us, west-us-coastal",
        choices=regions_dictionary.keys(),
        default=regions_dictionary.keys(),
    )
    parser.add_argument(
        "-f", "--features",
        nargs="+",
        metavar="feature",
        help="Space-separated list of features to download (default: ALL)."
             "\n Available features: wave-height, wind-speeds, wave-period, wave-direction",
        choices=features_dictionary.keys(),
        default=features_dictionary.keys(),
    )
    parser.add_argument("-n", "--no-data", action="store_true",
                        help="Do not download the 3-hourly wind and wave data (features specified by --features). "
                             "Only use this when downloading partition and/or buoy files.")
    parser.add_argument("-x", "--partition-files", action="store_true",
                        help="Additionally download monthly bulk spectral estimates for every region.")
    parser.add_argument("-y", "--buoy-files", action="store_true",
                        help="Additionally download monthly buoy files (see NOAA's hindcast webpage for specifics).")
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

        thread = threading.Thread(target=scrape_files, args=(start_month, threads_workload[i], args,))
        thread.start()
        threads.append(thread)
        start_month += threads_workload[i]

    # Show user the progress
    total_files = calculate_number_of_files()
    with alive_bar(total_files) as bar:
        last_processed_files = 0
        while True:
            # Update the bar based on number of processed files since last iteration
            for j in range(processed_files - last_processed_files):
                bar()
            last_processed_files = processed_files

            all_done = True  # Assume all threads are done
            for t in threads:
                if t.is_alive():
                    all_done = False  # If any thread is alive, not all are done
                    break
            if all_done:
                break  # Exit the while loop if all threads are finished

            time.sleep(1)  # Doesn't need to be the fastest bar in the world

    # Merge threads with main
    for thread in threads:
        thread.join()


counter_lock = threading.Lock()
processed_files = 0
regions_dictionary = {"alaska": "ak_4m",
                      "alaska-coastal": "ak_10m",
                      "east-us-coastal": "ecg_4m",
                      "east-us": "ecg_10m",
                      # grib folder uses "glo_30m_ext" and partitions folder uses "glo_30m"
                      "global": ("glo_30m_ext", "glo_30m"),
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
