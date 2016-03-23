#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yaml
import os
from boto.s3.connection import S3Connection


def client_project_keys():
    for k in BUCKET:
        if S3_PATH in str(k.key):
            yield k


def path_to_filename(path):
    '''
    Extract filename without file extension from
    path string.
    '''
    return os.path.basename(os.path.splitext(path)[0])

S3_FILE = open('cred.ini', 'r').read()
S3_BUCKET = 'oee'
S3_PATH = 'builditgreen/2016-02-17/hpxml'
CRED = yaml.load(S3_FILE)
CONN = S3Connection(CRED['aws_access_key'], CRED['aws_secret_key'])
BUCKET = CONN.get_bucket(S3_BUCKET)
