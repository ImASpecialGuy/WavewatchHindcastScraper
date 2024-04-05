
# WAVEWATCH III® 30-year Hindcast Phase 2 Data Downloading Utility

This project is a utility to help with downloading the files in https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2.php, and as such, you should go there to find out what the data is and how it is formatted.  
Without this utility, you would have to go into https://polar.ncep.noaa.gov/waves/hindcasts/nopp-phase2/ and download a file for every month, for every region, and for every feature.  
A map detailing where all the various regions are can be found in figure 3 of the validation paper (https://www.sciencedirect.com/science/article/pii/S1463500312001047?ref=cra_js_challenge&fr=RR-1).  


Note: the progress bar includes a naive time estimate that assumes all files are of equal size. Only trust its estimations when just downloading the standard 3-hourly data.

Note 2: before downloading a file, there is a check if the file already exists to prevent overwriting. If you want the files to be overwritten, you would have to delete them.

## Usage/Examples

### Download all features for several regions from Jan 2000 to Dec 2005
Note the regions argument (-r flag) can appear anywhere and can take many space-separated values.
```bash
python dataAcquisition.py -r alaska alaska-coastal pacific-islands -s 01-2000 -e 12-2005
```

### Download specific features for the global region from Jan 1979 to Dec 2009
```bash
python dataAcquisition.py -regions global -f wave-height wind-speeds
```

### Download bulk spectral estimates for all regions
This command downloads only the bulk spectral data (-x flag) from January 1979 until April 2005 (-e flag). The normal data (i.e. 3-hourly wave and wind data) is left out (-n).
```bash
python dataAcquisition.py -nxe 4-2005
```



