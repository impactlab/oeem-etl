from ..fetchers import fetch_all_customer_usage

class SaveCustomerUsage(luigi.Task):
    project_id = luigi.Parameter()
    usage_point_category = luigi.Parameter()
    min_date = luigi.DateParameter()
    max_date = luigi.DateParameter()
    xml = luigi.Parameter()

    def run(self):
        with self.output().open('w') as target:
            target.write(self.xml)

    def output(self):
        filename = '{}_{}_{}_{}.xml'.format(self.project_id
                                            self.usage_point_category,
                                            self.min_date.for_json(),
                                            self.max_date.for_json())
        # TODO: make base work with  target factory.
        return local.Target('data/consumption/raw/' + filename)

class FetchAllCustomers(luigi.WrapperTask):
    config = luigi.Parameter()
    customers = luigi.Parameter()
    target = luigi.Parameter()

    def requires(self):
        # TODO refactor fetch_all_customer_usage into a class?
        tasks = []
        for customer in customers:
            for up_cat, min_date, max_date, usage_xml in fetch_all_customer_usage(customer['subscription_id'],
                                                                                  customer['active_access_token'],
                                                                                  # TODO pass this in
                                                                                  self.espi):
                tasks.append(SaveCustomerUsage(customer['project_id'],
                                               up_cat, min_date,
                                               max_date, usage_xml))

        return tasks
