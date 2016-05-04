import yaml
import luigi

from ..fetchers import GreenButtonAppAPI
from ..fetchers import ESPICustomer
from ..storage import StorageClient


class FetchCustomerUsage(luigi.Task):
    customer_usage_point = luigi.Parameter()

    def run(self):
        xml = self.customer_usage_point.fetch_usage()
        with self.output().open('w') as f:
            f.write(xml)

    def output(self):
        filename = '{}_{}_{}.xml'.format(self.customer_usage_point.project_id,
                                         self.customer_usage_point.usage_point_id,
                                         self.customer_usage_point.run_num)

        return self.customer_usage_point.target(self.customer_usage_point.base_directory + filename)


class FetchAllCustomers(luigi.WrapperTask):
    config_path = luigi.Parameter(description="Path to settings")

    def _get_customer_auths(self, config):
        gb_client = GreenButtonAppAPI(config['green_button_api_url'],
                                      config['green_button_api_token'])

        client_customer_data = gb_client.get_client_customer_data(
                config['green_button_api_client_id'])

        customers_with_tokens = [
            customer for customer in client_customer_data
            if customer['access_token'] is not None
        ]

        return customers_with_tokens

    def _load_config(self, config_path):
        config = yaml.load(open(config_path, 'r').read())
        storage = StorageClient(config)
        config['base_directory'] = storage.get_base_directory('test/consumption/raw')
        config['existing_paths'] = storage.get_existing_paths('test/consumption/raw')
        config['target'] = storage.get_target_class()
        return config

    def requires(self):

        # adds a target using StorageClient class
        config = self._load_config(self.config_path)

        fetch_tasks = []
        for customer_cred in self._get_customer_auths(config):
            customer = ESPICustomer(customer_cred, config)
            for customer_usage_point in customer.usage_points():

                print(customer_usage_point.project_id)
                print(customer_usage_point.usage_point_id)

                if customer_usage_point.should_run():
                    task = FetchCustomerUsage(customer_usage_point)
                    fetch_tasks.append(task)

        return fetch_tasks
