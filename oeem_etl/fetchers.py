import yaml
import urllib
import requests
import datetime
from dateutil import parser
from dateutil.relativedelta import relativedelta
import xml.etree.cElementTree as etree


class GreenButtonAPI():
    '''
    Get ESPI access tokens for utility customers who have authorized our
    acess through GreenButton.
    '''

    def __init__(self, api_url, oauth_token):
        self.api_url = api_url
        self.headers = {"Authorization": "Bearer {}".format(oauth_token)}

    def fetch_client_customer_ids(self, client_id):
        '''
        Get customer ids for customers of given client that
        have authorized us to get their energy data.
        '''
        params = urllib.parse.urlencode({'oeeclient': client_id,
                                         'has_active_refresh_token': True,
                                         'id_only': True})
        customers = requests.get(self.api_url, params=params,
                                 headers=self.headers).json()
        return [str(customer['id']) for customer in customers[:10]]

    def client_customers(self, client_id):
        '''
        Return Green Button access token, subscriber id, join date,
        and other info for all customers of given client.
        '''
        for customer_id in self.fetch_client_customer_ids(client_id):
            customer_url = urllib.parse.urljoin(self.api_url, customer_id)
            customer = requests.get(customer_url, headers=self.headers).json()
            # Prevent ESPI API calls for customers with no access tokens.
            if customer['active_access_token'] is not None:
                # Datetime needed for downstream date comparison.
                customer['date_joined'] = parser.parse(customer['date_joined'])
                # Customer's subcription id lives in resource uri.
                customer['subscription_id'] = customer['resource_uri'].split('/')[-1]
                yield customer


class ESPIAPI():
    '''
    Get customer account and energy usage data from any ESPI-compliant
    utility API. Requires API url and ssl certificates, and customer-specific
    subscription ids and oauth access tokens.
    '''

    def __init__(self, api_url, cert):
        self.api_url = api_url
        self.cert = cert

    def make_headers(self, access_token):
        return {"Authorization": "Bearer {}".format(access_token)}

    def fetch_usage_points(self, subscription_id, access_token):
        '''
        Get the id and ServiceCategory kind of customer's usage points
        from ESPI API.
        '''
        endpoint = 'Subscription/{}/UsagePoint'.format(subscription_id)
        subscription_url = self.api_url + endpoint
        usage_point_xml = requests.get(subscription_url,
                                       headers=self.make_headers(access_token),
                                       cert=self.cert).text
        for usage_point_id, usage_point_kind in self.parse_usage_points(usage_point_xml):
            yield usage_point_id, usage_point_kind

    def parse_usage_points(self, usage_point_xml):
        '''
        Extract UsagePoint id and ServiceCategory kind from
        ESPI API XML response.
        '''
        # Parse XML element tree.
        root = etree.XML(usage_point_xml)
        # UsagePoint data is stored under entry elements in the Atom feed.
        for entry_tag in root.findall('{http://www.w3.org/2005/Atom}entry'):
            # UsagePoint id only provided in entry link elements.
            # Always grab it from the href attribute of the second link.
            usage_point_id = entry_tag.findall('{http://www.w3.org/2005/Atom}link')[1].attrib['href'].split('/')[-1]
            # Grab UsagePoint kind from the text inside entry.content.UsagePoint.ServiceCategory.kind
            usage_point_kind = entry_tag.find('.//{http://naesb.org/espi}kind').text
            yield usage_point_id, usage_point_kind

    def fetch_usage_data2(self, subscription_id, usage_point_id, access_token, min_date, max_date):
        '''
        Get customer usage point's usage data from ESPI API.
        '''
        print('OMG', min_date, max_date)
        params = urllib.parse.urlencode({'published-min': min_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                                         'published-max': max_date.strftime('%Y-%m-%dT%H:%M:%SZ')})
        endpoint = 'Batch/Subscription/{}/UsagePoint/{}'.format(subscription_id,
                                                                usage_point_id)
        usage_url = self.api_url + endpoint + '?' + params
        return requests.get(usage_url,
                            headers=self.make_headers(access_token),
                            cert=self.cert).text

    def fetch_usage_data(self, subscription_id, usage_point_id, access_token, date_range):
        '''
        Get customer usage point's usage data from ESPI API.
        '''
        now = datetime.datetime.utcnow()
        max_date = now.strftime('%Y-%m-%dT%H:%M:%SZ')
        if date_range == 'all_data':
            min_date = (now - relativedelta(years=4)).strftime('%Y-%m-%dT%H:%M:%SZ')  # Works for golden to 2/25/2012. Only returns a little over two months of data, though. Is this all we have, or is it limited? How far back can you go for someone?
        elif date_range == 'new_data':
            min_date = (now - relativedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')  # Seems to get 3 days before, not 1. But it does return one day of data.
        else:
            raise ValueError('Please enter a valid date_range option string.')
        params = urllib.parse.urlencode({'published-min': min_date,
                                         'published-max': max_date})
        endpoint = 'Batch/Subscription/{}/UsagePoint/{}'.format(subscription_id,
                                                                usage_point_id)
        usage_url = self.api_url + endpoint + '?' + params
        return requests.get(usage_url,
                            headers=self.make_headers(access_token),
                            cert=self.cert).text

    def fetch_customer_usage(self, subscription_id, access_token, date_range):
        '''
        Get all usage data for customer, across all usage points, from
        ESPI API. Save files to disk.
        # TODO: SINGLE OUTPUT FILE FOR ALL USAGE POINTS?
        '''
        for usage_point_id, usage_point_cat in self.fetch_usage_points(subscription_id, access_token):
            yield self.fetch_usage_data(subscription_id, usage_point_id,
                                         access_token, date_range)

    def fetch_customer_account(self, subscription_id, access_token):
        '''Get customer name and address for ESPI API.'''
        endpoint = 'Batch/RetailCustomer/'
        account_url = self.api_url + endpoint + subscription_id
        return requests.get(account_url, headers=self.make_headers(access_token),
                            cert=self.cert).text

# Initialize Green Button API fetcher.
cred = yaml.load(open('cred.ini', 'r').read())
AUTH_API_URL = cred['oauth_service_url']
AUTH_API_TOKEN = cred['oauth_service_token']
green_button = GreenButtonAPI(AUTH_API_URL, AUTH_API_TOKEN)

# Initialize ESPI API fetcher.
ESPI_API_URL = cred['espi_api_url']
CERT_FILE = cred['cert_file']
PRIVKEY_FILE = cred['privkey_file']
espi = ESPIAPI(ESPI_API_URL, cert=(CERT_FILE, PRIVKEY_FILE))
