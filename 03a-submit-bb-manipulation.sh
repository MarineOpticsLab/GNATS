#!/bin/bash

#PBS -N bb-manipulation
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

python $scriptDir/x03a-shift-bb-data.py --uwfile $dataDir/01b-underway-formatted-gnats.csv --bbcycleParametersFile $dataDir/x01-pH-cycle-duration-limits-dict.pickle --cruiseNameColumn CruiseName --datetimeColumn UWTime --numSamplesColumn numSamples --ofileShiftedData $dataDir/01c-underway-shifted-gnats.csv

python $scriptDir/x03b-calculate-bbprime-average.py --uwfile $dataDir/01c-underway-shifted-gnats.csv --cruiseNameColumn CruiseName --datetimeColumn UWTime --bbtotColumn bbtot532 --numSamplesColumn numSamples --bbtotStdColumn bbtot532Std --bbacidColumn bbacid --bbacidStdColumn bbacidStd --bbprimeColumn bbprime --bbprimeStd bbprimeStd --ofileAveragedBB $dataDir/01d-underway-averaged-bb-gnats.csv