# Shared ETL utility functions
This is a library of routines for writing ETL (Extract-Transform-Load) pipelines to get data into OEEM Datastore instances. The goal is to store reusable logic that will speed up the ingestion of data into a datastore.

Specifically, this repo includes:
- functions for fetching energy data from ESPI APIs, saving it to S3 or GCS, and uploading it to an OEEM Datastore instance. 
- [luigi tasks](luigi.readthedocs.org) for fetching, parsing, and upload energy consumption and retrofit project data. These tasks, and the functions they rely on, are designed to be reused across client-specific ETL projects.


## Installation
- For development of the ETL library: first, clone the repo. Then, make a new virtualenv and install `requirements.txt`.
- For development of a new client ETL: make a new repo with the following name format: `oeem-etl-<clientname>`. This ETL library repo is available on PyPi, make sure to add it to that client ETL repo's requirements, or you won't be able to use this library.

## Running it
This library isn't run directly. Rather, it's components are used to build datastore-specifiec ETL pipelines. At OEE, those are separate codebases living in separate repos.

To run a pipeline using luigi..

First, Make sure you're in the virtual environment defined by that pipeline's `requirements.txt`

Next, open up a shell and type `luigid` to start the luigi server on your laptop.

In a separate shell, use the luigi command line tool to kick off the pipeline:
`PYTHONPATH=. luigi --module fetch_client_data FetchAllCustomers --config-path config.yaml`
- the `--module` flag points luigi to the module where your pipeline lives. 
- `FetchAllCustomers` is the name of the final luigi task we want to execute. It must be contained in `module`.
- `--config_path` is a luigi parameter that FetchAllCustomers needs to execute correctly. Different luigi tasks have different parameters, this is just a way of passing in a parameter from the command line.
- the `PYTHONPATH=.` bit adds the current directory to the python path, so the luigi pipeline executes correctly. [See here](https://github.com/spotify/luigi/issues/1321) for more info.

Luigi spins up, figures out which tasks in your pipeline haven't completed yet, and schedule them for execution. To see the pipeline in action, point your browser to `http://localhost:8082/`.

## Overview
As you've likely gathered by now, our ETL pipelines are orchestrated by luigi. The best way to learn about luigi is to work through [the tutorial](http://luigi.readthedocs.io/en/stable/example_top_artists.html).

Luigi allows you to define Directed Acyclic Graphs (DAGs) of tasks. A task takes data as input and produces data as output, which may be the input of another task. All output datasets are stored on an object store, S3 or Google Cloud Storage (the code allows us to abstract away the storage service lives in `oeem_etl/storage`). 

The typical ETL pipeline consists of two DAGs: fetch and parse/upload. 

### Parse/upload DAG
All clients have a parse/upload DAG - to get raw project and consumption data from your storage service, parse it using client-specific parsers, and upload it to the datastore using an uploader module. The luigi parse tasks live at `oeem_etl/tasks/parse.py`. Parsers are defined in the client luigi script, though consumption data in ESPI xml format parsed with a parser from the `eemeter` package. The parse tasks use parsers to convert raw data into predictable csv format that is saved back to the storage service. 

These parsed files are then fetched from the storage service, concatenated into a single stream, and uploaded in bulk in one or more POST requests to a user-specified datastore by the uploader module. The luigi upload tasks live at `oeem_etl/tasks/upload.py`, using uploader code which lives at `oeem_etl/uploader`. 

Project and consumption data must already be in storage for this DAG to work. For some clients, the data is uploaded manually. For others, we get it from APIs. Enter: the fetch DAG.

### Fetch DAG
Currently, we build a fetch DAG only for clients for which we get consumption data from an ESPI API. The luigi tasks that make up this DAG live in `oeem_etl/tasks/fetch.py`. The class that does all the heavy lifting of talking to the API and getting the right data for a customer given what we've downloaded before for them in `ESPICustomer`, and it can be doing in `oeem_etl/fetchers.py`. 

*Green Button Auth Service*
This DAG starts by getting a list of customers who have authorized green button from OEE's green button authentication service.

*New customers*
If we've never gotten data for that customer before (because, perhaps, they authorized access after our last fetch run) then we get that last 4 years of data from the API. 

*Existing customers*
If we HAVE gotten data for them before, we only ask for new data since last time the ETL was run. We do this by fetching the customer's last "run" of data - the data file we downloaded for them last time - figuring out the "max date" in this data - the final time interval that we have energy values for - and setting is as the "min" date for our request. The result: we get the very next time interval, with no gaps in our energy records arising between fetch runs. (Technically, we fetch fetch data for a customer's usage point - either electric or gas consumption - not the customer as a whole. A customer might only have a single usage point, we data history might start at different dates depending on the usage point, and so on. `customer-usagepoints` are the core unit of the fetch DAG.)

All fetch data ends up in the storage service at `<client-bucket-name>/consumption/raw/<project-id>_<usage-point-id>_<run-number>.xml`, where `<project_id>` is the customer's project_id, `<usage-point-id>` is the ESPI identifier of one of the customer's "usage points" (either their electricity or gas lines), and `<run-number>` is an integer reflecting how many times the fetch has run for this customer-usage point. For example, the very first time your run number will be 0, the second time it will be 1, and so on. We parameterize the filenames in this way so that luigi creates a new file on disk to correspond with every run of the DAG. Run numbers are used internally by the DAG to determine what data to ask for. Consistent filenames for these raw dataset are crucial - the ETL logic relies on it to the core.

Some additional notes on the fetch DAG:
- if no new data is available for a customer-usagepoint since the last fetch run, no data is downloaded, and no new raw file is created in the storage service. In other words, the `run number` for that customer-usagepoint stays where it is, instead of incrementing with the new data. This functionality is crucial to fetch DAG working correctly.
- raw datasets are all saved to a directory, usually `consumption/raw` (though this can be overwritten in the config file) at which point the parse/upload DAG takes over.
- The two DAGs are completely decoupled: the fetch DAG should run one a day, getting whatever new data is can for people. The parse/upload DAG can run more frequently, looking at the raw data dir (and knowing nothing about the fetch DAG) and parsing/uploading whatever new files appear.


## Tricky bits of the ETL pipeline
Here's a brain dump of explanations about tricky parts of the codebase:
- Run numbers in filenames must be accurate for the whole thing to work.
- Run numbers for the current run are inferred form previously downloaded data: if there are no previous files for the current customer-usagepoint, run number is 0. If there are previous files, run number if the last run number + 1.
- Besides computing the run number of the fetch before creating a `FetchCustomerUsage` for the given customer-usagepoint, we also need to figure out if the run should take place at all. The reason this check exists is just in case the fetch DAG is rerun before new data is available. If you have not downloaded previous raw files for the customer-usagepoint, then "should_run" is always true - go ahead and get the data. If you have, though, then call `_should_run_now`, which checks that the max-date of the data currently available on the API is greater than the max-date of the data you got on the last run. If it is, go ahead and generate a new `FetchCustomerUsage` usage task, increment the run number, fetch the data, and have the luigi fetch task save the data at filename with that run number (`<project-id>_<usage-pointid->_<run-number>.xml`). If not, then don't even generate the luigi task, so we don't risk getting duplicate data from the API - or no data at all, if the API doesn't like the min and max date params you give it - and saving it to disk under an incremented run number. Why does this matter? Because next time you run the fetch DAG, it will get the run number and the min_date for the next fetch based on the data you got last time, so you don't want this data to be invalid, or skewed on time, so that you don't get any gaps in coverage between files.
- We fetch data for a given customer-usagepoint through the ESPI API using "min-date" and "max-date" params. The API behavior is a little weird. Every ESPI API has a particular UTC hour that represents the end of full day of data. For PG&E in May 2016, that time is UTC+7:00, which is equivalent to 00:00 PDT. If you ask for data before this "end of day time", say at 6:59:59 UTC, the API will return the previous 24 hours of data. If you set min-date to some point after this hour, say 7:01, you will get data from that day and not the previous day. Similarly, if you set max date to 6:59:59, you will only get data from the day that's ending. If you set it at 7:01, you will also get data for the next day. Unless, of course, that data isn't available through the API yet, because you're setting max-date to the current time and data simply isn't available yet. Typically, if you're asking for data after the "end of day time" - UTC 7:00 today - you'll won't get data for yesterday, or even the day before, but usually the day before the day before yesterday, with the max-date being today at UTC 7:00 minus 2 days. (max date of today at UTC 7:00am would give you back yesterday's energy data, if it is available)
- That's important background knowledge for working with the `_get_min_date` and `_get_max_date` methods of `ESPICustomer`, which are responsible for generating the right min and max date parameters for the current fetch task. Here's how `get_min_date` works: if you've never gotten data for that customer before, get the max date and subtract four years. If you have, get the max_date of the data you got during the last run, and that's your min date for the next request.
- `get_max_date` takes the current datetime, subtracts two days from it, and sets the time at 1 seconds before whatever the "end of day hour is". (If the day "ends" at 7:00UTC, the time is set to 6:59:59.) The idea for going back a couple days is that it increases the odds of there being new data available for that customer-usagepoint.
- Another tricky bit of `ESPICustomer` happens in the `usage_points` method. The method returns the `ESPICustomer` instance - yes, the very instance on which you are calling the method - only with the `usage_point_id` and `usage_point_cat` attributes updated internally. Luigi then takes that return `ESPICustomer` instance, and uses it to paramterize a `FetchCustomerUsage` task. If a customer has multiple usagepoints, multiple `FetchCustomerUsage` tasks will be generated for them in the fetch DAG. If that methods returns `self`, then both of these tasks will be paramterized by the same exact CustomerUsage instance, with the same usage_point attribute. This is bad. So instead, we copy the object before returning it, to make luigi work.

## How to ETL a new client
- First, make a new repo.
- Then, make a `config.yaml` file. This will contain credentials for your storage service, green button auth, ESPI API, and datastore. It also includes the client's name and id. The ETL library is written in such a way that most of the work in a new ETL is just making a correct configuration file - the code in the library is generic, and designed to work with a wide range of clients.
- parse/upload: make a new module called `parse_and_upload_client_data.py` that imports the `UploadDatasets` luigi task from `oeem_etl/tasks/upload`. Write a `project_parser` and `consumption_parser`, optionally using some of the functions in `eemeter`. most of the actual coding work involves creating these parsers. You then pass them on the `UploadDatasets` DAG, along with a path to the `config.yaml` file. Run this pipeline with `PYTHONPATH=. luigi --module parse_and_upload_client_data.py RunPipeline --config-path config.yaml`. For an example, look at the `oeem-etl-renewfinancial` repo.
- fetch: for the fetch DAG, start by creating a module called `fetch_client_data.py`. Then just import `from oeem_etl.tasks.fetch import FetchAllCustomers`. Run this pipeline with `PYTHONPATH=. luigi --module fetch_client_data FetchAllCustomers --config-path config.yaml`. For an example, look at the `oeem-etl-renewfinancial` repo.
