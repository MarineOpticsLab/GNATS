#!/bin/bash

#PBS -N field-merge
#PBS -q route

#PBS -l ncpus=32,mem=256gb
#PBS -l walltime=4:00:00
#PBS -o /mnt/storage/labs/mitchell/projects/nasacms2018/analysis/data/gnatsat_workflow/logs
#PBS -e /mnt/storage/labs/mitchell/projects/nasacms2018/analysis/data/gnatsat_workflow/logs

# Load modules and environment
module use /mod/bigelow
module load anaconda3
source activate ~/sunnysenv

scriptDir=/mnt/storage/labs/mitchell/spinkham/gitHubRepos/cms_dev/field-workflow
dataDir=/mnt/storage/labs/mitchell/projects/nasacms2018/analysis/data/gnatsat_workflow

python $scriptDir/x04-merge-field-data.py --uwFile $dataDir/01d-underway-averaged-bb-gnats.csv --discreteFile $dataDir/02b-discrete-formatted-gnats.csv --uwIdCol UWid --discreteIdCol StationDataID --uwCruiseNameCol CruiseName --discreteCruiseNameCol CruiseName --uwTimeCol UWTime --discreteTimeCol StationTime --uwLongitudeCol UWLongitude --discreteLongitudeCol Longitude --uwLatitudeCol UWLatitude --discreteLatitudeCol Latitude --discreteShallowestCol Shallowest --discreteDepthCol Depth --ofileFieldData $dataDir/04-merged-field-data.csv