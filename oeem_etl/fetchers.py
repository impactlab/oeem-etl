import os
import arrow
import urllib
import requests
import logging
import py
from dateutil import parser
import xml
import xml.etree.cElementTree as etree
import copy

class GreenButtonAppAPI():
    '''
    Get ESPI access tokens for utility customers who have authorized our
    acess through GreenButton.
    '''

    def __init__(self, base_url, oauth_token):
        self.base_url = base_url
        self.headers = {"Authorization": "Bearer {}".format(oauth_token)}

    def get_client_customer_ids(self, client_id):
        '''
        Get customer ids for customers of given client (by client_id) that
        have authorized us to get their energy data.
        '''
        response = requests.get(
                urllib.parse.urljoin(self.base_url, "/api/v1/pgecustomers/"),
                params={
                    'oeeclient': client_id,
                    'has_active_refresh_token': True,
                    'id_only': True,
                },
                headers=self.headers)

        if response.status_code == 200:
            customers = response.json()
        else:
            raise ValueError(response)
        return [str(c["id"]) for c in customers]

    def get_customer_data(self, customer_id):
        """
        Returns raw customer data for a particular pge customer by pk.
        """
        customer_url = urllib.parse.urljoin(self.base_url,
                "/api/v1/pgecustomers/{}".format(customer_id))
        response = requests.get(customer_url, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(response)

    def get_client_customer_data(self, client_id):
        '''
        Return Green Button access token, subscriber id, join date,
        and other info for all customers of given client.
        '''

        for customer_id in self.get_client_customer_ids(client_id):

            customer = self.get_customer_data(customer_id)

            # Customer's subcription id lives in resource uri
            # Use None if parse fails.
            try:
                subscription_id = customer['resource_uri'].split('/')[-1]
            except AttributeError:
                subscription_id = None

            data = {
                "subscription_id": subscription_id,
                "access_token": customer["active_access_token"],
                "project_id": customer["project_id"],
            }

            yield data


class ESPICustomer():
    '''
    Get customer-usage point energy usage data from any ESPI-compliant
    utility API. Requires API url and ssl certificates, and customer-specific
    subscription ids and oauth access tokens.
    '''

    def __init__(self, customer_cred, config):

        # Customer attributes
        self.project_id = customer_cred['project_id']
        self.subscription_id = customer_cred['subscription_id']
        self.access_token = customer_cred['access_token']

        # Crosswalk for usage data parsing.
        self.usage_point_cat_to_fuel_type = {'0': ('electricity', 'kWh'),
                                             '1': ('natural_gas', 'therms')}

        # Bucket where client customer data lives.
        self.existing_paths = config['existing_paths']
        self.base_directory = config['base_directory']


        # luigi Target class so we can read/write to paths.
        self.target = config['target']

        # ESPI API credentials.
        self.api_url = config['espi_api_url']
        self.cert = (config['cert_file'], config['privkey_file'])

    def __repr__(self):
        try:
            msg = "<project_id: {}, usage_point_id: {}, usage_point_cat: {}, run_num: {}>"
            return msg.format(self.project_id, self.usage_point_id,
                              self.usage_point_cat, self.run_num)
        except AttributeError:
            return "<project_id: {}>".format(self.project_id)

    ###################
    # UTILITY METHODS #
    ###################
    def _make_headers(self, access_token):
        '''Make headers for EPSI data requests.'''
        return {"Authorization": "Bearer {}".format(access_token)}

    def _path_to_run_num(self, path):
        '''Extract the run number from a usage file path.'''
        return py.path.local(path).purebasename.split('_')[-1]

    def _check_dates(self, request_date, response_date, date_type):
        '''Check whether the date your asked for is the date you got.'''
        try:
            # Data params in the API are inclusive. If your day starts at 0700,
            # and you set max-date to 0700, you'll get back the day.
            # To get the previous day, set it to 06:59:59. The max end date
            # returned will be 0700, however, so you must add 1 second to
            # the request time to see if you get the date you asked for.
            if date_type == 'max':
                request_date = request_date.replace(seconds=+1)
            # Also, usage point cat 1 starts one second after the hour.
            assert request_date == response_date
        except AssertionError:
            msg = "Request %s_date for subscription %s, usage point %s doesn't match response: %s %s" % (date_type, self.subscription_id,
                                                                                                         self.usage_point_id, request_date, response_date)
            logging.warn(msg)

    #########################################################
    # INTERNAL METHODS - FETCH USAGE POINTS AND CONSUMPTION #
    #########################################################
    def _fetch_usage_points(self):
        '''
        Fetch the customers usage points, return each point's
        id and ServiceCategory kind from the ESPI API.
        '''
        endpoint = 'Subscription/{}/UsagePoint'.format(self.subscription_id)
        subscription_url = self.api_url + endpoint
        usage_point_xml = requests.get(subscription_url,
                                       headers=self._make_headers(self.access_token),
                                       cert=self.cert).text

        for usage_point_id, usage_point_cat \
                in self._parse_usage_points(usage_point_xml):
            yield usage_point_id, usage_point_cat

    def _parse_usage_points(self, usage_point_xml):
        '''
        Extract UsagePoint id and ServiceCategory kind from
        ESPI API XML response.
        '''
        # Parse XML element tree.
        try:
            root = etree.XML(usage_point_xml)
        except xml.etree.ElementTree.ParseError as e:
            print("Couldn't parse xml!")
            print(usage_point_xml)
            return

        # UsagePoint data is stored under entry elements in the Atom feed.
        for entry_tag in root.findall('{http://www.w3.org/2005/Atom}entry'):

            # UsagePoint id only provided in entry link elements.
            # Always grab it from the href attribute of the second link.
            usage_point_id = entry_tag.findall('{http://www.w3.org/2005/Atom}link')[1].attrib['href'].split('/')[-1]

            # Grab UsagePoint kind from the text inside entry.content.UsagePoint.ServiceCategory.kind
            usage_point_kind = entry_tag.find('.//{http://naesb.org/espi}kind').text

            yield usage_point_id, usage_point_kind

    def _fetch_usage_data(self, min_date, max_date):
        '''
        Get customer usage point's usage data from ESPI API.
        '''
        params = urllib.parse.urlencode({'published-min': min_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                         'published-max': max_date.strftime('%Y-%m-%dT%H:%M:%SZ')})
        endpoint = 'Batch/Subscription/{}/UsagePoint/{}'.format(self.subscription_id,
                                                                self.usage_point_id)
        usage_url = self.api_url + endpoint + '?' + params
        return requests.get(usage_url,
                            headers=self._make_headers(self.access_token),
                            cert=self.cert).text

    def _parse_usage_data(self, usage_xml):
        '''
        Parse EPSI usage XML data into format suitable for OEEM uploader.
        '''
        try:
            root = etree.XML(usage_xml)  # Parse XML element tree.
        except xml.etree.ElementTree.ParseError as e:
            print("Couldn't parse xml!")
            print(usage_xml)
            return []

        # IntervalReading elements house all energy data.
        reading_tag = './/{http://naesb.org/espi}IntervalReading'
        interval_readings = root.findall(reading_tag)
        return [self._parse_reading(reading) for reading
                in interval_readings]

    def _parse_reading(self, reading):
        '''
        Parse EPSI IntervalReading XML element into OEEM uploader-compliant
        dictionary.
        '''
        start = int(reading.find('.//{http://naesb.org/espi}start').text)
        duration = int(reading.find('.//{http://naesb.org/espi}duration').text)
        end = start + duration
        fuel_type, unit_name = self.usage_point_cat_to_fuel_type[self.usage_point_cat]
        value = reading.find('{http://naesb.org/espi}value').text
        return {'project_id': False,
                'start': start,
                'end': end,
                'fuel_type': fuel_type,
                'unit_name': unit_name,
                'value': value}

    def _find_reading_date_range(self, readings):
        '''
        Find the date range represents by a series of readings:
        the earliest min-date to the latest max-date.
        '''
        try:
            min_start = readings[0]["start"]
            max_end = readings[0]["end"]
        except IndexError:
            return None, None

        for reading in readings:
            if reading['start'] < min_start:
                min_start = reading['start']
            if reading['end'] > max_end:
                max_end = reading['end']
        return arrow.get(min_start), arrow.get(max_end)

    ###############################################
    # INTERNAL METHODS - "SHOULD WE FETCH?" LOGIC #
    ###############################################
    def _get_previous_downloads(self):
        '''
        Return full paths of usage dataset in storage service bucket
        that belong to the given project-usage point combination.
        e.g. These are the paths to all the files we've downloaded of
        J Smith's gas usage.
        '''
        for path in self.existing_paths:

            # Parse filename, see if it belongs to project-usage point.
            filename = os.path.basename(path)
            path_project_id, path_usage_point_id, run_num = filename.split('_')

            if str(self.project_id) == path_project_id \
               and str(self.usage_point_id) == path_usage_point_id:
                yield path

    def _get_last_run_num(self, paths):
        '''
        Get the most recent run number for a set of usage file paths.
        NOTE: this function assumes the paths all belong to for a single
        customer-usage point.
        '''
        run_numbers = [self._path_to_run_num(path) for path in paths]
        return int(sorted(run_numbers)[-1])

    def _should_run_now(self):
        '''
        If the max_date you'd get if you fetched data now is
        newer than the max_date we got on the last run, you
        should run now. If not, you shouldn't.
        '''
        max_date_if_run_now = self._get_max_date_if_run_now()
        max_date_last_run = self._fetch_last_run_max_date()
        if max_date_last_run is None \
                or max_date_if_run_now is None:
            return True
        return max_date_last_run < max_date_if_run_now

    def _get_max_date_if_run_now(self):
        '''
        Returns max date you'd get if you fetched now by actually
        fetching data with the currently computed max date param
        and then checking the returned readings.
        '''
        max_date = self._get_max_date()
        usage_xml = self._fetch_usage_data(max_date.replace(days=-2, seconds=+1),
                                           max_date)
        # TODO: make this single function?
        readings = self._parse_usage_data(usage_xml)
        actual_min_date, actual_max_date = self._find_reading_date_range(readings)
        return actual_max_date

    def _fetch_last_run_max_date(self):
        '''
        Fetch the max_date of the data we downloaded on the last run.
        NOTE: should only get called if data has been previously fetched
        for customer-usage_point_id.
        '''
        # If we already downloaded data, current request should ask for
        # period starting immediately after the end of the last request.
        last_run_num = int(self.run_num) - 1
        filename = '{}_{}_{}.xml'.format(self.project_id,
                                         self.usage_point_id,
                                         last_run_num)

        path = os.path.join(self.base_directory, filename)
        with self.target(path).open('r') as f:
            usage_xml = f.read()

        readings = self._parse_usage_data(usage_xml)
        actual_min_date, actual_max_date = self._find_reading_date_range(readings)
        return actual_max_date

    ###########################################################
    # INTERNAL METHODS - "WHICH DATES SHOULD WE FETCH?" LOGIC #
    ###########################################################
    def _get_min_date(self):
        '''
        Get min date for API call. If we've previously downloaded
        data for the subscriber/usage point, pick up where we left off.
        Otherwise, go back as far as you can.
        '''
        # If we already downloaded data, current request should ask for
        # period starting immediately after the end of the last request.
        if self.run_num != 0:
            max_date_last_run = self._fetch_last_run_max_date()
            # Assumes that previous API calls always get full 24 hours
            # of data, from 07:00:00 (or 7:00:01, if usage cat 1) on
            # day t, to the same time on day t+1.
            # TODO: generalize for all ESPI APIs.
            assert max_date_last_run.hour == 7 \
                and max_date_last_run.minute == 0 \
                and (max_date_last_run.second == 0 \
                     or max_date_last_run.second == 1)
            return max_date_last_run
        # If not, get everything we can. (4 years from PG&E).
        else:
            # Time in max date is one second before next day, must
            # add 1s so it can serve as min date.
            # i.e. 06:59:59 (end of one day) -> 07:00:00 (start of next one)
            return self._get_max_date().replace(years=-4, seconds=+1)

    def _get_max_date(self, current_datetime=arrow.utcnow(),
                      end_day_hour=7, hour_data_updated=18):
        '''
        Return the max date for which we can get data: either
        yesterday's data, if it has been made available already,
        or the previous day's data.

        By default, assume that data was gathered in US/Pacific,
        so the max date for yesterday's data it today at 06:59 hours,
        since US/Pacific is usually UTC - 7 hours. Also,
        assume that data is made available 2 hours after
        day ends, by default.

        TODO: make this daylight savings resilient...
        TODO: check that a day of data always end at 0700,
        regardless of time of year. (looks like it doesn't.. used to
        end at 0800.)
        TODO: double check when hour data is updated. (looks like it
        doesn't all get updated at once, and not until sometime after 3pm..)
        '''
        # Time today that yesterday's data is made available.
        data_refresh_time = current_datetime.replace(hour=hour_data_updated,
                                                     minute=0, second=0,
                                                     microsecond=0)
        # If yesterday's data has been made available...
        if current_datetime > data_refresh_time:
            # Return max date that will retrieve yesterday's data,
            # likely uploaded this morning.
            max_date = data_refresh_time.replace(hour=end_day_hour - 1,
                                                 minute=59, second=59)
            # ^^^ If you set max-date one second after this, ^^^
            # you might get the next day.
        # If it hasn't been made available yet...
        else:
            # Return max date for will retrieve the day before yesterday's data,
            # likely uploaded yesterday.
            max_date = data_refresh_time.replace(days=-1,
                                                 hour=end_day_hour - 1,
                                                 minute=59, second=59)
        # Usage category 1 starts at 07:00:01, so set max date to 7:00:00.
        if self.usage_point_cat == '1':
            max_date = max_date.replace(seconds=+1)
        return max_date

    ###############
    # API METHODS #
    ###############
    def usage_points(self):
        '''
        Get customer usage points. Loop through points, update customer
        usage point attributes on instance, then yield the updated instance.
        Effectively, this method allows you to iterate through versions
        of the parent EPSICustomer class, preloaded with different usage
        points, which affects the behavior of most core methods.
        '''
        for usage_point_id, usage_point_cat in self._fetch_usage_points():

            # Must return a copy of the instance with mutated usage
            # point fields, or luigi will end up creating all
            # FetchCustomerUsage tasks for the customer with the same
            # final ESPICustomer instance.
            customer_usage_point = copy.copy(self)
            customer_usage_point.usage_point_id = usage_point_id
            customer_usage_point.usage_point_cat = usage_point_cat
            yield customer_usage_point


    def should_run(self):
        '''
        Figure out if we should fetch new data for the customer
        usage point, and generate the run number for the fetch if so.
        '''
        previous_downloads = list(self._get_previous_downloads())
        # If fetch has never run for this customer-up, run for the first time.
        if len(previous_downloads) == 0:
            self.run_num = 0
            should_run = True
        # If not, run fetch if you haven't already fetched data available
        # right now.
        else:
            self.run_num = self._get_last_run_num(previous_downloads) + 1
            should_run = self._should_run_now()
        print(self.run_num, should_run)
        return should_run


    def fetch_usage(self):
        '''
        Fetch usage for this customer-usage point, either last four years
        or only what's new since last fetch.
        '''

        # Figure out what date range to pull for this subscriber usage point.
        # Will download last 4 years for new customers, new data since
        # last download for existing customers.
        print('FETCHING ' + self.project_id + ' ' + self.usage_point_cat + ' ' + str(self.run_num))

        min_date = self._get_min_date()
        max_date = self._get_max_date()

        # Get the usage data.
        usage_xml = self._fetch_usage_data(min_date, max_date)

        # Find actual min and max date.
        readings = self._parse_usage_data(usage_xml)

        # Only proceed if you got readings back.
        if len(readings) > 0:

            actual_min_date, actual_max_date = self._find_reading_date_range(readings)

            # Log if we got back usage data for a different day than we asked for.
            self._check_dates(min_date, actual_min_date, 'min')
            self._check_dates(max_date, actual_max_date, 'max')

            # Return actual dates you got data back for, just in case
            # customer doesn't have data going all the way back, or
            # your max-date is earlier than you asked for because
            # data isn't available yet.
            return usage_xml
        else:
            msg = 'No readings for {} {} from {} to {}'
            logging.warn(msg.format(self.subscription_id, self.usage_point_cat,
                                    min_date, max_date))
            return None
