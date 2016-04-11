import os
import py

import arrow

from oeem_etl.fetchers import *
from oeem_etl import fetchers

###########################
# ESPI API max date tests #
###########################
# Each utility makes data for the previous day available at a certain time of day.
# If we're after that "date update" time, we should set the max-date parameter
# on API queries to fetch yesterday's data. That means setting max-date to one
# second before the day "ends" in UTC time.
# If we're before that time, we should get the day before yesterday's data,
# because that's all that's been made available so far.
def test_get_max_date_before_data_refresh():
    test_date = arrow.get(2016, 5, 26, 7, 59, 59)
    max_date = get_max_date(current_datetime=test_date, end_of_day_hour_in_local_time=6, hour_data_updated=8)
    assert max_date == arrow.get(2016, 5, 25, 5, 59, 59)


def test_get_max_date_after_data_refresh():
    test_date = arrow.get(2016, 5, 26, 8, 0, 1)
    max_date = get_max_date(current_datetime=test_date, end_of_day_hour_in_local_time=6, hour_data_updated=8)
    assert max_date == arrow.get(2016, 5, 26, 5, 59, 59)


###########################
# ESPI API min date tests #
###########################
def test_get_previous_downloads(tmpdir):
    # Filename components.
    sub_id_1 = '15939'
    sub_id_2 = '31402'
    up_cat_1 = '0'
    up_cat_2 = '1'
    min_date = arrow.get(2015, 1, 1).strftime('%Y-%m-%dT%H:%M:%SZ')
    max_date = arrow.get(2015, 1, 2).strftime('%Y-%m-%dT%H:%M:%SZ')

    # Add three filenames to temporary fs,
    # Example filename: 75917_0_2016-03-29T00:52:28.763313+00:00_2016-03-30T00:52:28.763337+00:00.xml
    user_fn = "{}_{}_{}_{}.xml".format(sub_id_1, up_cat_1, min_date, max_date)
    tmpdir.join(user_fn).write('')  # Must write to save files.
    tmpdir.join("{}_{}_{}_{}.xml".format(sub_id_2, up_cat_1, min_date, max_date)).write('')
    tmpdir.join("{}_{}_{}_{}.xml".format(sub_id_1, up_cat_2, min_date, max_date)).write('')
    bucket = [path.strpath for path in tmpdir.listdir()]

    # Test!
    downloads = list(get_previous_downloads(sub_id_1, up_cat_1, bucket))
    assert len(downloads) == 1
    assert py.path.local(downloads[0]).basename == user_fn


def test_get_last_download_date(tmpdir):
    sub_id_1 = '31402'
    up_cat_1 = '0'
    min_date = arrow.get(2015, 1, 1)
    max_date = arrow.get(2015, 1, 2)

    # Add three filenames to temporary fs,
    fn_1 = "{}_{}_{}_{}.xml".format(sub_id_1,
                                    up_cat_1,
                                    min_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    max_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
    fn_2 = "{}_{}_{}_{}.xml".format(sub_id_1,
                                    up_cat_1,
                                    min_date.replace(day=2).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    max_date.replace(day=3).strftime('%Y-%m-%dT%H:%M:%SZ'))
    fn_3 = "{}_{}_{}_{}.xml".format(sub_id_1,
                                    up_cat_1,
                                    min_date.replace(day=3).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    max_date.replace(day=4).strftime('%Y-%m-%dT%H:%M:%SZ'))
    tmpdir.join(fn_1).write('')  # Must write to save files.
    tmpdir.join(fn_2).write('')
    tmpdir.join(fn_3).write('')
    paths = [path.strpath for path in tmpdir.listdir()]
    assert get_last_download_date(paths) == max_date.replace(day=4)


def test_path_to_max_date():
    sub_id_1 = '31402'
    up_cat_1 = '0'
    min_date = arrow.get(2015, 1, 1).strftime('%Y-%m-%dT%H:%M:%SZ')
    max_date = arrow.get(2015, 1, 2).strftime('%Y-%m-%dT%H:%M:%SZ')
    path = "/foo/bar/{}_{}_{}_{}.xml".format(sub_id_1, up_cat_1,
                                             min_date, max_date)
    assert path_to_max_date(path) == arrow.get(2015, 1, 2)


def test_get_min_date_existing_subscriber(tmpdir):
    '''Get the min date for an existing subscriber with three previous
    downloads. Should find biggest max-date of the previously downloaded
    usage files, and then pick the next day.'''
    sub_id_1 = '31402'
    sub_id_2 = '12405'
    up_cat_1 = '0'
    min_date = arrow.get(2015, 1, 1, 7, 0)
    max_date = arrow.get(2015, 1, 2, 7, 0)

    fn_1 = "{}_{}_{}_{}.xml".format(sub_id_1,
                                    up_cat_1,
                                    min_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    max_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
    fn_2 = "{}_{}_{}_{}.xml".format(sub_id_1,
                                    up_cat_1,
                                    min_date.replace(day=2).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    max_date.replace(day=3).strftime('%Y-%m-%dT%H:%M:%SZ'))
    fn_3 = "{}_{}_{}_{}.xml".format(sub_id_1,
                                    up_cat_1,
                                    min_date.replace(day=3).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    max_date.replace(day=4).strftime('%Y-%m-%dT%H:%M:%SZ'))
    fn_4 = "{}_{}_{}_{}.xml".format(sub_id_2,
                                    up_cat_1,
                                    min_date.replace(day=4).strftime('%Y-%m-%dT%H:%M:%SZ'),
                                    max_date.replace(day=5).strftime('%Y-%m-%dT%H:%M:%SZ'))
    tmpdir.join(fn_1).write('')  # Must write to save files.
    tmpdir.join(fn_2).write('')
    tmpdir.join(fn_3).write('')
    tmpdir.join(fn_4).write('')
    bucket = [path.strpath for path in tmpdir.listdir()]

    assert get_min_date(sub_id_1, up_cat_1, bucket) == arrow.get(2015, 1, 4, 7, 0)


def test_get_min_date_new_subscriber(monkeypatch):
    sub_id_1 = '31402'
    up_cat_1 = '0'
    max_date = arrow.get(2015, 1, 2, 6, 59)
    monkeypatch.setattr(fetchers, 'get_max_date', lambda: max_date)
    bucket = []
    assert_date = arrow.get(2011, 1, 2, 7, 0)
    assert get_min_date(sub_id_1, up_cat_1, bucket) == assert_date


def test_find_reading_date_range():
    readings = [{'start': arrow.get(2014, 1, 2), 'end': arrow.get(2014, 1, 3)},
                {'start': arrow.get(2016, 7, 2), 'end': arrow.get(2016, 7, 13)},
                {'start': arrow.get(2011, 3, 14), 'end': arrow.get(2011, 3, 16)},
                {'start': arrow.get(2012, 5, 12), 'end': arrow.get(2012, 5, 14)}]
    assert arrow.get(2011, 3, 14) == find_reading_date_range(readings)[0]
    assert arrow.get(2016, 7, 13) == find_reading_date_range(readings)[1]
