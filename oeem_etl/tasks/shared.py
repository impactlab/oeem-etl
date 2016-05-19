'''
Shared helper functions
'''

import os

def mangle_path(path, target, replacement):
    '''
    Create parsed file path that corresponds to raw file.

    Example:
    <client-bucket>/projects/{target}/ABC/2015-06-01.xml
    <client-bucket>/projects/{replacement}/ABC/2015-06-01.csv
    '''
    split_path_raw = path.split('/')

    # Change parent dir from target to replacement, but allow subdirectories.
    split_path_parsed = []
    found = False # split only once, so track whether or not it's been done
    for part in split_path_raw:
        if part == target and not found:
            found = True
            split_path_parsed.append(replacement)
        else:
            split_path_parsed.append(part)

    if not found:
        message = (
            'Path "{}" does not contain the directory "{}"'
            .format(path, target)
        )
        raise ValueError(message)

    # Change file extension to csv.
    filename = os.path.splitext(split_path_parsed[-1])[0]
    split_path_parsed[-1] = filename + '.csv'

    return '/'.join(split_path_parsed)
