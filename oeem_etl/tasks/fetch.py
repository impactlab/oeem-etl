import luigi
from ..fetchers import fetch_customer_usage


class FetchCustomerUsage(luigi.Task):
    customer = luigi.Parameter()
    usage_point = luigi.Parameter()
    run_num = luigi.Parameter()
    # TODO remove this
    espi_client = luigi.Parameter()
    target = luigi.Parameter()

    def run(self):
        xml = fetch_customer_usage(self.customer, self.usage_point, self.run_num, self.target)
        with self.output().open('w') as f:
            f.write(self.xml)

    def output(self):
        filename = '{}_{}_{}.xml'.format(self.customer['project_id'],
                                         self.usage_point['id'], self.run_num)
        # TODO fix this
        return self.target('gs://oeem-renew-financial-data/' + 'consumption/test2/' + filename)


class FetchAllCustomers(luigi.WrapperTask):
    customers = luigi.Parameter()
    raw_consumption_paths = luigi.Parameter()
    espi_client = luigi.Parameter()
    target = luigi.Parameter()

    def requires(self):
        for customer in self.customers:
            for usage_point in self.espi_client.fetch_usage_points(customer):
                run_num = get_last_run_num(customer, usage_point, raw_consumption_paths)
                FetchCustomerUsage(customer, usage_point, run_num, self.espi_client, self.target())
