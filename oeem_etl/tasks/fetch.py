import yaml
import luigi

from ..fetchers import GreenButtonAPI
from ..fetchers import ESPICustomer
from ..storage import StorageClient


class FetchCustomerUsage(luigi.Task):
    customer_up = luigi.Parameter()
    usage_point_id = luigi.Parameter()  # TODO DELETE

    def run(self):
        xml = self.customer_up.fetch_usage()
        with self.output().open('w') as f:
            f.write(xml)

    def output(self):
        filename = '{}_{}_{}.xml'.format(self.customer_up.project_id,
                                         self.customer_up.usage_point_id,
                                         self.customer_up.run_num)
        # EPSICustomer needs to store the Target class for internal
        # methods, get it from the instance attr instead of passing
        # an additional luigi param.
        # TODO fix this
        return self.customer_up.target('gs://oeem-renew-financial-data/' + 'consumption/test/' + filename)


class FetchAllCustomers(luigi.WrapperTask):
    config_path = luigi.Parameter(description="Path to settings")

    def _get_customer_auths(self, config):
        gb_client = GreenButtonAPI(config['green_button_api_url'],
                                   config['green_button_api_token'])
        return list(gb_client.client_customers(config['client_id']))

    def _load_config(self, config_path):
        config = yaml.load(open(config_path, 'r').read())
        storage = StorageClient(config)
        config['bucket'] = storage.get_dataset_paths('consumption')
        config['target'] = storage.get_target()
        return config

    def requires(self):
        config = self._load_config(self.config_path)
        fetch_tasks = []
        for customer_cred in self._get_customer_auths(config):
            customer = ESPICustomer(customer_cred, config)
            for customer_up in customer.usage_points():
                print(customer_up.project_id)
                print(customer_up.usage_point_cat)
                if customer_up.should_run():
                    fetch_tasks.append(FetchCustomerUsage(customer_up, customer_up.usage_point_id))
        return fetch_tasks
