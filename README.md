# GNATS

### Overview:

This repository houses scripts relevant to the processing of both GNATS flow-through and discrete data.

### Sub-Directories:

**matlab-scripts:**  These scripts take in raw instrument data and apply factory, instrument, and cruise-specific calibrations, as well as data dependent corrections (ie. temp-sal or scattering corrections). These scripts are used to update the private SQL GNATS database managed by BLOS. GNATS data is publicly available on SeaBASS at DOI: 10.5067/SeaBASS/GNATS/DATA001.

**main:** The main directory houses mostly a python workflow which aligns and merges the discrete data with the flow-through data based on nearest time. Additionally, data is visualized and qc performed. Data are flagged. Flagged data are eliminated from the final flow-discrete gnats compiled dataset.