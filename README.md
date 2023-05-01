**SRP 04/14/2023**

I have created this subdirectory in order to create an organized field data workflow for the creation of GNATSAT.

The cms_dev parent directory was created before the matchup-workflow was generated.  Notebooks in this workflow dependended on a G2 Reader database extraction. However, since then, the database has been migrated to being hosted on Digital Ocean.  Additionally, we have moved away from the G2 Reader to using python to extract data from the database.  The python extraction formats the data differently than the G2 reader extraction. Therefore, notebooks that dealt with field data need to be modified. 

Ultimately, we have decided on an organization as follows:

Field data will be configured in the specific project directory (ie. gnatsat field data will be configured in the cms_dev, and pic data will be configured in the pic_dev repository). Once the field data is extracted and configured, the creation of a seabass file, of downloading satellite data, and of merging field data with satellite data will be done in the matchup_workflow repository. 

In this directory, I will extract data from the database via python and configure the data for merging with satellite data.