# Shared ETL utility functions
This is a shared library of routines for writing ETL pipelines for OEEM Datastore instances. Features include:

- fetch energy data from ESPI APIs
- GCS support
- OEEM datastore loading

## Install

    git clone https://github.com/impactlab/oeem-etl
    cd oeem-etl
    pip install -e .

## Configure

Running a Luigi task requires a `.cfg` configuration file with the following section and parameters:

    [oeem]
    url=http://localhost:9009/
    access_token=tokstr
    project_owner=1
    file_storage=local
    local_data_directory=/test_data
    uploaded_base_path=uploaded
    formatted_base_path=oeem-format

## Run

This library isn't run directly. Rather, it's components are used to build datastore-specifiec ETL pipelines, which are 
found in separate repos. 

An example of a command to run a Luigi task from a client-specific module:

    export LUIGI_CONFIG_PATH=/test_data/config.cfg
    luigi --module oeem_etl_client LoadAll --local-scheduler

The client module that implements the task should be installed where Luigi can find it (e.g. `pip install -e .`).

***

**The following is historical documentation which is somewhat misleading and inaccurate as the module has evolved (it will be revised at a later date).**

## Overview

Luigi allows you to define Directed Acyclic Graphs (DAGs) of tasks. A task takes data as input and produces data as output, which may be the input of another task. All output datasets are stored on an object store, S3 or Google Cloud Storage (the code allows us to abstract away the storage service lives in `oeem_etl/storage`).

The typical ETL pipeline consists of two groups of tasks: fetch and parse/upload.

### Parse/upload tasks

All clients have a parse/upload task, which gets raw project and consumption data from your storage service, parses it using client-specific parsers, and uploads it to the datastore using an uploader module. The luigi parse tasks live at `oeem_etl/tasks/parse.py`. Parsers are can be defined in the client luigi script (though consumption data in ESPI xml format parsed with a parser from the `eemeter` package). The parse tasks use parsers to convert raw data into predictable csv format that is saved back to the storage service.

These parsed files are then fetched from the storage service, concatenated into a single stream, and uploaded in bulk in one or more POST requests to a user-specified datastore by the uploader module. The luigi upload tasks live at `oeem_etl/tasks/upload.py`, using uploader code which lives at `oeem_etl/uploader`.

Project and consumption data must already be in storage for parse/upload to work. For some clients, the data is uploaded manually. For others, we get it from APIs with fetch tasks.

### Fetch tasks

Currently, we run fetch tasks only for clients that provide consumption data from an ESPI API. These luigi tasks live in `oeem_etl/tasks/fetch.py`. The `ESPICustomer` (`oeem_etl/fetchers.py`) class talks to the API and gets the right data for a customer given what we've downloaded before for them.

*Green Button Auth Service*
This task starts by getting a list of customers who have authorized green button from OEE's green button authentication service.

*New customers*
If we've never gotten data for that customer before (because, perhaps, they authorized access after our last fetch run) then we get that last 4 years of data from the API.

*Existing customers*
If we HAVE gotten data for them before, we only ask for new data since last time the ETL was run. We do this by fetching the customer's last "run" of data - the data file we downloaded for them last time - figuring out the "max date" in this data - the final time interval that we have energy values for - and setting is as the "min" date for our request. The result: we get the very next time interval, with no gaps in our energy records arising between fetch runs. (Technically, we fetch fetch data for a customer's usage point - either electric or gas consumption - not the customer as a whole. A customer might only have a single usage point, we data history might start at different dates depending on the usage point, and so on. `customer-usagepoints` are the core unit of the fetch DAG.)

All fetch data ends up in the storage service at `<client-bucket-name>/consumption/raw/<project-id>_<usage-point-id>_<run-number>.xml`, where `<project_id>` is the customer's project_id, `<usage-point-id>` is the ESPI identifier of one of the customer's "usage points" (either their electricity or gas lines), and `<run-number>` is an integer reflecting how many times the fetch has run for this customer-usage point. For example, the very first time your run number will be 0, the second time it will be 1, and so on. We parameterize the filenames in this way so that luigi creates a new file on disk to correspond with every run of the DAG. Run numbers are used internally by the task to determine what data to ask for. Consistent filenames for these raw dataset are crucial - the ETL logic relies on it to the core.

Some additional notes on fetch tasks:

- if no new data is available for a customer-usagepoint since the last fetch run, no data is downloaded, and no new raw file is created in the storage service. In other words, the `run number` for that customer-usagepoint stays where it is, instead of incrementing with the new data. This functionality is crucial to fetch DAG working correctly.
- raw datasets are all saved to a directory, usually `consumption/raw` (though this can be overwritten in the config file) at which point the parse/upload DAG takes over.
- The two tasks are completely decoupled: the fetch task should run one a day, getting whatever new data is can for people. The parse/upload task can run more frequently, looking at the raw data dir (and knowing nothing about the fetch DAG) and parsing/uploading whatever new files appear.


## Tricky bits of the ETL pipeline

Here's a brain dump of explanations about tricky parts of the codebase:

- Run numbers in filenames must be accurate for the whole thing to work.
- Run numbers for the current run are inferred form previously downloaded data: if there are no previous files for the current customer-usagepoint, run number is 0. If there are previous files, run number if the last run number + 1.
- Besides computing the run number of the fetch before creating a `FetchCustomerUsage` for the given customer-usagepoint, we also need to figure out if the run should take place at all. The reason this check exists is just in case the fetch DAG is rerun before new data is available. If you have not downloaded previous raw files for the customer-usagepoint, then "should_run" is always true - go ahead and get the data. If you have, though, then call `_should_run_now`, which checks that the max-date of the data currently available on the API is greater than the max-date of the data you got on the last run. If it is, go ahead and generate a new `FetchCustomerUsage` usage task, increment the run number, fetch the data, and have the luigi fetch task save the data at filename with that run number (`<project-id>_<usage-pointid->_<run-number>.xml`). If not, then don't even generate the luigi task, so we don't risk getting duplicate data from the API - or no data at all, if the API doesn't like the min and max date params you give it - and saving it to disk under an incremented run number. Why does this matter? Because next time you run the fetch DAG, it will get the run number and the min_date for the next fetch based on the data you got last time, so you don't want this data to be invalid, or skewed on time, so that you don't get any gaps in coverage between files.
- We fetch data for a given customer-usagepoint through the ESPI API using "min-date" and "max-date" params. The API behavior is a little weird. Every ESPI API has a particular UTC hour that represents the end of full day of data. For PG&E in May 2016, that time is UTC+7:00, which is equivalent to 00:00 PDT. If you ask for data before this "end of day time", say at 6:59:59 UTC, the API will return the previous 24 hours of data. If you set min-date to some point after this hour, say 7:01, you will get data from that day and not the previous day. Similarly, if you set max date to 6:59:59, you will only get data from the day that's ending. If you set it at 7:01, you will also get data for the next day. Unless, of course, that data isn't available through the API yet, because you're setting max-date to the current time and data simply isn't available yet. Typically, if you're asking for data after the "end of day time" - UTC 7:00 today - you'll won't get data for yesterday, or even the day before, but usually the day before the day before yesterday, with the max-date being today at UTC 7:00 minus 2 days. (max date of today at UTC 7:00am would give you back yesterday's energy data, if it is available)
- That's important background knowledge for working with the `_get_min_date` and `_get_max_date` methods of `ESPICustomer`, which are responsible for generating the right min and max date parameters for the current fetch task. Here's how `get_min_date` works: if you've never gotten data for that customer before, get the max date and subtract four years. If you have, get the max_date of the data you got during the last run, and that's your min date for the next request.
- `get_max_date` takes the current datetime, subtracts two days from it, and sets the time at 1 seconds before whatever the "end of day hour is". (If the day "ends" at 7:00UTC, the time is set to 6:59:59.) The idea for going back a couple days is that it increases the odds of there being new data available for that customer-usagepoint.
- Another tricky bit of `ESPICustomer` happens in the `usage_points` method. The method returns the `ESPICustomer` instance - yes, the very instance on which you are calling the method - only with the `usage_point_id` and `usage_point_cat` attributes updated internally. Luigi then takes that return `ESPICustomer` instance, and uses it to paramterize a `FetchCustomerUsage` task. If a customer has multiple usagepoints, multiple `FetchCustomerUsage` tasks will be generated for them in the fetch DAG. If that methods returns `self`, then both of these tasks will be paramterized by the same exact CustomerUsage instance, with the same usage_point attribute. This is bad. So instead, we copy the object before returning it, to make luigi work.

