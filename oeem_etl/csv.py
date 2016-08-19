import csv

def date_reader(date_format):
    def reader(raw): 
        if raw.strip() == '':
            return None
        return datetime.datetime.strptime(raw, date_format)
    return reader

def date_formatter(date_format):
    def formatter(timestamp):
        if timestamp is None:
            return ''
        return timestamp.strftime(date_format)
    return formatter

def read_csv_file(csvfile, dtypes=None):
    """Read the csv file, possibly converting values in its cells

    Arguments:

        csvfile: filelike object
        dtypes: dictionary of functions to cast read str to desired data type

    Returns: list of rows in csv file
    """
    if dtypes is None:
        dtypes = {}
    def apply_dtypes(row):
        for key, value in row.items():
            if key in dtypes:
                row[key] = dtypes[key](value)
        return row
    reader = csv.DictReader(csvfile)
    result = [apply_dtypes(row) for row in reader]
    return result

def write_csv_file(csvfile, records, fieldnames, formatters=None):
    """
    Arguments:

        file: file-like object to write to
        records: dicts to write
        fieldnames: list of columns to write
    """
    if formatters is None:
            formatters = {}
    def format_row(row, formatters):
        for key, value in row.items():
            if key in formatters:
                row[key] = formatters[key](value)
        return row

    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    for record in records:
        writer.writerow(format_row(record, formatters))