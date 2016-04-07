import os
import py

import arrow

from .fetchers import get_max_date, get_previous_downloads

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
    assert max_date == arrow.get(2016, 5, 25, 5, 59)


def test_get_max_date_after_data_refresh():
    test_date = arrow.get(2016, 5, 26, 8, 0, 1)
    max_date = get_max_date(current_datetime=test_date, end_of_day_hour_in_local_time=6, hour_data_updated=8)
    assert max_date == arrow.get(2016, 5, 26, 5, 59)


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
    tmpdir.join(fn_1).write('')  # Must write to save files.
    tmpdir.join("{}_{}_{}_{}.xml".format(sub_id_2, up_cat_1, min_date, max_date)).write('')
    tmpdir.join("{}_{}_{}_{}.xml".format(sub_id_1, up_cat_2, min_date, max_date)).write('')
    bucket = [path.strpath for path in tmpdir.listdir()]

    # Test!
    downloads = list(get_previous_downloads(sub_id_1, up_cat_1, bucket))
    assert len(downloads) == 1
    assert py.path.local(downloads[0]).basename == user_fn
