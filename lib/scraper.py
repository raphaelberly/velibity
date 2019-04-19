import logging
import re
from datetime import datetime
from time import sleep

from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)


class VelibScraper(object):

    def __init__(self, driver, credentials, urls):
        self.driver = driver
        self._credentials = credentials

        self.urls = urls

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
        password.send_keys(self._credentials['password'])
        username.send_keys(self._credentials['username'])
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
                LOGGER.info(f'Loading content from page {button_value}')
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
                    'timestamp': self._get_timestamp(trip),
                    'distance': self._get_distance(trip),
                    'duration': self._get_duration(trip),
                }
                yield parsed_trip

    def run(self):
        self.login()
        content_generator = self.content_loader()
        trip_generator = self.content_parser(content_generator)

        for trip in trip_generator:
            print(trip)
