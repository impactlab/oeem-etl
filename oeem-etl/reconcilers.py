#!/usr/bin/env python
# -*- utf-8 -*-

import yaml

# TODO: rename things, refactor code
# TODO: hardcode this to work with danny.

cred = yaml.load(open('cred.ini', 'r').read())


def make_key(account):
    return ' '.join([account['address'], account['city'], account['state'], account['zipcode']])


def make_symbol_table(consumption):
    table = {}
    for record in consumption:
        account = record['account']
        key = make_key(account)
        table[key] = record['usage']
    return table


def add_project_id_to_consumption(usage_data, project_id):
    for service_cat in usage_data:  # List service cat readings.
        for reading in service_cat:
            reading['project_id'] = project_id
    return usage_data


def link_projects_to_consumption(projects, consumption, danny_mode=True):
    '''
    NOTE: all projects returned, with or without baseline
    start and reporting end dates. Only consumption records
    that match to a project are returned.
    '''
    table = make_symbol_table(consumption)
    projects_out = []
    consumption_out = []
    for project in projects:
        key = make_key(project['customer'])
        # If danny mode is on, assign danny's
        # address as a key to the first project.
        if danny_mode:
            key = '201 n park ct martinez ca 94553'
            danny_mode = False
        usage_data = table.get(key, False)
        if usage_data:
            project_id = project['project']['project_id']
            clean_usage_data = add_project_id_to_consumption(usage_data, project_id)
            consumption_out.append(clean_usage_data)
        projects_out.append(project)
    return projects_out, consumption_out
