from bs4 import BeautifulSoup
import requests

# base URL that contains all the files
URL = "https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/"
page = requests.get(URL)

print(page.text)
