from setuptools import setup

long_description = '''Utilities to facilitate data ETL for OEEM Datastore. Includes
                   components to fetch energy usage and retrofit project data,
                   connect projects to usage records, and upload them to the
                   datastore.'''
setup(
    name='oeem_etl',
    version='0.1.2',
    description='Open Energy Efficiency Meter ETL utils',
    long_description=long_description,
    url='https://github.com/impactlab/oeem-etl/',
    author='Juan-Pablo Velez, Phil Ngo',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    scripts=['bin/oeem-upload'],
    install_requires=[
        'eemeter',
        'luigi',
        'boto',
        'py',
        'google-api-python-client',
        'oauth2client',
        'requests>=2.10.0'
    ],
    keywords='open energy efficiency meter etl espi greenbutton',
)
