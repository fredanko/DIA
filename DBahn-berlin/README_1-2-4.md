# Installation steps for task 1, 2, 4 dependencies

Requirements for tasks 1,2 and 4 can be installed together.

One installation option can be found in the following steps:

In the repository, enter:

#### Create the environment
`conda env create -f environment.yml`

#### Activate it
`conda activate dia_etl_env`

If graph-tool is not automatically installed, please use the following command or refer to the [installation instructions](https://graph-tool.skewed.de/installation.html).
`conda install -c conda-forge graph-tool`

#### Connect data sources

The uploaded folder does not contain the timetable data due to hand-in constraints. For a working pipeline, the original directories `timetables` and `timetable_changes` need to lie at the same level as the provided directory structure offered, so in `DBahn-berlin`, at the same level as the task folders (1, 2, 4).



#### Solutions

The ``src`` folder contains some helper python methods, the ``sql`` folder contains all queries needed. The solutions are contained in the respective notebooks in the folders ``taskX``.

#### ENV file

Please note that in order to connect to your local postgres DB, you will have to use an ``.env`` file or modify your postgres password in ``src/timetable_etl/config.py``.