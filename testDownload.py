import requests
from pathlib import Path

url = "https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/197910/gribs/multi_reanal.pi_10m.hs.197910.grb2"
path = Path("data/197910/gribs")
file_name = Path("multi_reanal.pi_10m.hs.197910.grb2")

path.mkdir(parents=True, exist_ok=True)


def download_file(file_url, download_folder):
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an exception for bad response codes

    with open(str(download_folder / file_url.split('/')[-1]), "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            # if chunk:  # Filter out keep-alive chunks
            file.write(chunk)


download_file(url, path)
