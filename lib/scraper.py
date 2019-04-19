import logging
import re
from datetime import datetime
from functools import partial
from time import sleep

import psycopg2
from bs4 import BeautifulSoup
from toolz import partition_all
from tqdm import tqdm

LOGGER = logging.getLogger(__name__)

BATCH_SIZE = 10


class VelibScraper(object):

    def __init__(self, driver, username, credentials, urls, table_name):
        self.driver = driver
        self._credentials = credentials

        self.username = username
        self.urls = urls
        self.table_name = table_name

    # GET A WEBPAGE
    def get_page(self, url):
        self.driver.get(url)

    # GET SOUP FROM CURRENT WEBPAGE
    def get_soup(self):
        return BeautifulSoup(self.driver.page_source, 'html.parser')

    # LOG IN TO THE WEBSITE
    def login(self):
        LOGGER.info('Logging in')
        # Get the login page
        self.get_page(url=self.urls['login'])
        # Submit the credentials
        username = self.driver.find_element_by_name("_username")
        password = self.driver.find_element_by_name("_password")
        username.send_keys(self._credentials['users'][self.username]['username'])
        password.send_keys(self._credentials['users'][self.username]['password'])
        password.submit()
        sleep(2)

    def content_loader(self):
        # Load URLs page
        self.get_page(self.urls['trips'])
        sleep(10)
        # Click on each page button successively and yield resulting page source
        page_buttons = self.driver.find_elements_by_class_name('page-item')
        for button in page_buttons:
            button_value = button.text.strip()
            try:
                button_value = int(button_value[0])
            except ValueError:
                pass
            else:
                LOGGER.debug(f'Loading content from page {button_value}')
                button.click()
                sleep(1)
                yield self.get_soup()

    @staticmethod
    def _find_div_params(class_name):
        return {'name': 'div', 'attrs': {'class': class_name}}

    def _get_timestamp(self, trip):
        string = trip.find(**self._find_div_params('operation-date')).getText().strip()
        return datetime.strptime(string, '%d/%m/%Y - %H:%M')

    def _get_distance(self, trip):
        string = trip.find(**self._find_div_params('col-3 col-lg runs-item font-weight-bold')).getText().strip()
        return float(string[:-2].replace(',', '.'))

    def _get_duration(self, trip):
        string = trip.find(**self._find_div_params('col-3 col-lg-3 runs-item font-weight-bold')).getText().strip()
        search = re.search(r'((?P<min>\d*)(min))? ?((?P<sec>\d*)(sec)?)', string)
        return int(search.group('min') or 0) * 60 + int(search.group('sec') or 0)

    def content_parser(self, content_generator):
        # Parse each page successively
        for page_soup in content_generator:
            trips = page_soup.findAll('div', attrs={'class': 'container runs'})
            for trip in trips:
                parsed_trip = {
                    'username': self.username,
                    'datetime': self._get_timestamp(trip),
                    'distance_km': self._get_distance(trip),
                    'duration_s': self._get_duration(trip),
                }
                yield parsed_trip

    @staticmethod
    def _get_connection_string(host, port, db, user, password):
        return f"host='{host}' dbname='{db}' port={port} user='{user}' password='{password}'"

    def _drop_existing(self, conn):
        LOGGER.info(f'Deleting existing data')
        cur = conn.cursor()
        query = f"DELETE FROM {self.table_name} WHERE username='{self.username}'"
        cur.execute(query)
        cur.close()

    @staticmethod
    def _get_insert_query(table_name, trip_dict):
        key_str = ','.join(trip_dict.keys())
        val_str = "'{}'".format("','".join([str(val) for val in trip_dict.values()]))
        return f'INSERT INTO {table_name} ({key_str}) VALUES ({val_str});'

    def trips_uploader(self, values_generator, batch_size=1):
        with psycopg2.connect(self._get_connection_string(**self._credentials['db'])) as conn:
            # Erase day if already existing in the database
            self._drop_existing(conn)
            # Take care of logging
            LOGGER.info('Scraping data and uploading it to database')
            tqdm_kwargs = {'desc': '> scraping', 'unit': ' trips processed'}
            tqdm_values_generator = tqdm(values_generator, **tqdm_kwargs)
            # Insert values by batch
            _get_insert_query = partial(self._get_insert_query, self.table_name)
            query_generator = map(_get_insert_query, tqdm_values_generator)
            query_batch_generator = partition_all(batch_size, query_generator)
            i = 0
            for query_batch in query_batch_generator:
                cur = conn.cursor()
                cur.execute('\n'.join(query_batch))
                cur.close()
                i += len(query_batch)
            LOGGER.info(f'Data from {i} trips was scraped and sent to database')

    def run(self):
        self.login()
        content_generator = self.content_loader()
        trip_generator = self.content_parser(content_generator)
        self.trips_uploader(trip_generator, BATCH_SIZE)
