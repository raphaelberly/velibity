import logging

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

