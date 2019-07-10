import logging
import re
from base64 import b64decode
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

        self.last_trip_datetime = self._get_last_trip_datetime()

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
        username.send_keys(self._credentials['website']['username'])
        password.send_keys(b64decode(self._credentials['website']['password']).decode('utf-8'))
        password.submit()
        sleep(8)
        # Assert logged in
        if 'error.login' in self.driver.current_url:
            raise PermissionError(f'Could not log in with user "{self.username}"')

    def logout(self):
        LOGGER.info('Logging out')
        self.get_page(url=self.urls['logout'])
        sleep(10)

    def content_loader(self):
        # Load URLs page
        self.get_page(self.urls['trips'])
        sleep(12)
        i = 1
        # Click on "Next" button and yield until it is deactivated
        while True:
            # Find "Next" button
            next_button = self.driver.find_elements_by_class_name('page-link')[-2]
            assert next_button.text.strip() == '»', 'Could not find "Next" button on page'
            LOGGER.debug(f'Loading content from page {i}')
            yield self.get_soup()
            # Find disabled buttons
            disabled_buttons = self.driver.find_elements_by_class_name('disabled')
            disabled_buttons_value = [item.text.strip() for item in disabled_buttons]
            if '»' in disabled_buttons_value:
                break
            else:
                next_button.click()
                i += 1
                sleep(5)

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
        search = re.search(r'((?P<h>\d*)(h))? ?((?P<min>\d*)(min))? ?((?P<sec>\d*)(sec))?', string)
        return int(search.group('h') or 0) * 3600 + int(search.group('min') or 0) * 60 + int(search.group('sec') or 0)

    @staticmethod
    def _get_bike_type(trip):
        return trip.find(name='img', attrs={'class': 'velo_elec_bleu'}) is not None

    def content_parser(self, content_generator):
        stop = False
        # Parse each page successively
        for page_soup in content_generator:
            if stop is True:
                break
            trips = page_soup.findAll('div', attrs={'class': 'container runs'})
            for trip in trips:
                parsed_trip = {
                    'username': self.username,
                    'start_datetime': self._get_timestamp(trip),
                    'distance_km': self._get_distance(trip),
                    'duration_s': self._get_duration(trip),
                    'is_elec': self._get_bike_type(trip),
                }
                if self.last_trip_datetime and parsed_trip['start_datetime'] <= self.last_trip_datetime:
                    stop = True
                    break
                else:
                    yield parsed_trip

    @staticmethod
    def _get_connection_string(host, port, db, user, password):
        return f"host='{host}' dbname='{db}' port={port} user='{user}' password='{password}'"

    def _get_last_trip_datetime(self):
        with psycopg2.connect(self._get_connection_string(**self._credentials['db'])) as conn:
            cur = conn.cursor()
            query = f"SELECT max(start_datetime) FROM {self.table_name} WHERE username='{self.username}'"
            cur.execute(query)
            return cur.fetchone()[0]

    @staticmethod
    def _get_insert_query(table_name, trip_dict):
        key_str = ','.join(trip_dict.keys())
        val_str = "'{}'".format("','".join([str(val) for val in trip_dict.values()]))
        return f'INSERT INTO {table_name} ({key_str}) VALUES ({val_str});'

    def trips_uploader(self, values_generator, batch_size=1):
        with psycopg2.connect(self._get_connection_string(**self._credentials['db'])) as conn:
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
        self.logout()
