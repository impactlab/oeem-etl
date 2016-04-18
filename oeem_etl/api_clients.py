import yaml
import urllib
import requests
from datetime import datetime, timedelta
from dateutil import parser
import xml.etree.cElementTree as etree


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
        return [c["id"] for c in customers]

    def get_customer_data(self, customer_id):
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

            # Datetime needed for downstream date comparison.
            customer['date_joined'] = parser.parse(customer['date_joined'])

            # Customer's subcription id lives in resource uri.
            # None if resource_uri is None
            try:
                customer['subscription_id'] = customer['resource_uri'].split('/')[-1]
            except AttributeError:
                customer['subscription_id'] = None

            yield customer


class PGEAPI():
    '''
    Get customer account and energy usage data from the PGE API.
    Requires API url and ssl certificates, and customer-specific
    subscription ids and oauth access tokens.
    '''

    def __init__(self, base_url, cert):

        if base_url.endswith("/"):
            self.base_url = base_url[:-1]
        else:
            self.base_url = base_url

        self.cert = cert

    def _make_headers(self, access_token):
        return {"Authorization": "Bearer {}".format(access_token)}

    def _parse_usage_points(self, subscription_xml):
        '''
        Extract UsagePointId
        '''
        # Parse XML element tree.
        try:
            root = etree.XML(subscription_xml)
        except:
            import pdb;pdb.set_trace()

        # UsagePoint data is stored under entry elements in the Atom feed.
        for entry_tag in root.findall('{http://www.w3.org/2005/Atom}entry'):

            usage_point_id = entry_tag.findall(
                    '{http://www.w3.org/2005/Atom}link')[1].attrib['href'].split('/')[-1]

            yield usage_point_id

    def get_usage_point_ids(self, subscription_id, access_token):
        '''
        Get the id and ServiceCategory kind of customer's usage points
        from ESPI API.
        '''
        url = '{}/Subscription/{}/UsagePoint'.format(self.base_url,
                subscription_id)
        subscription_xml = requests.get(url,
                                       headers=self._make_headers(access_token),
                                       cert=self.cert).text
        for usage_point_id in self._parse_usage_points(subscription_xml):
            yield usage_point_id

    def get_usage_point_xml(self, subscription_id, usage_point_id,
            access_token, min_date=None, max_date=None):
        '''
        Get customer usage point's usage data from ESPI API.
        '''
        url = "{}/Batch/Subscription/{}/UsagePoint/{}".format(self.base_url,
                subscription_id, usage_point_id)


        now = datetime.utcnow()


        if min_date is None:
            min_date = now - timedelta(days=365*5)

        if max_date is None:
            max_date = now

        params = {
            'published-min': min_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'published-max': max_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
        }

        response = requests.get(url, params=params,
                headers=self._make_headers(access_token),
                cert=self.cert)

        return response.text

    # def fetch_usage_data(self, subscription_id, usage_point_id, access_token, date_range):
    #     '''
    #     Get customer usage point's usage data from ESPI API.
    #     '''
    #     now = datetime.datetime.utcnow()
    #     max_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    #     if date_range == 'all_data':
    #         min_date = (now - relativedelta(years=4)).strftime('%Y-%m-%dT%H:%M:%SZ')  # Works for golden to 2/25/2012. Only returns a little over two months of data, though. Is this all we have, or is it limited? How far back can you go for someone?
    #     elif date_range == 'new_data':
    #         min_date = (now - relativedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')  # Seems to get 3 days before, not 1. But it does return one day of data.
    #     else:
    #         raise ValueError('Please enter a valid date_range option string.')
    #     params = urllib.parse.urlencode({'published-min': min_date,
    #                                      'published-max': max_date})
    #     endpoint = 'Batch/Subscription/{}/UsagePoint/{}'.format(subscription_id,
    #                                                             usage_point_id)
    #     usage_url = self.api_url + endpoint + '?' + params
    #     return requests.get(usage_url,
    #                         headers=self.make_headers(access_token),
    #                         cert=self.cert).text
    #
    # def fetch_customer_usage(self, subscription_id, access_token, date_range):
    #     '''
    #     Get all usage data for customer, across all usage points, from
    #     ESPI API. Save files to disk.
    #     # TODO: SINGLE OUTPUT FILE FOR ALL USAGE POINTS?
    #     '''
    #     for usage_point_id, usage_point_cat in self.fetch_usage_points(subscription_id, access_token):
    #         yield self.fetch_usage_data(subscription_id, usage_point_id,
    #                                      access_token, date_range)
    #
    # def fetch_customer_account(self, subscription_id, access_token):
    #     '''Get customer name and address for ESPI API.'''
    #     endpoint = 'Batch/RetailCustomer/'
    #     account_url = self.api_url + endpoint + subscription_id
    #     return requests.get(account_url, headers=self.make_headers(access_token),
    #                         cert=self.cert).text
