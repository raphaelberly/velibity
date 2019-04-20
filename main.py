import argparse
import logging
from copy import copy

import yaml

from lib.driver import run_driver
from lib.logging import configure_logging
from lib.scraper import VelibScraper

# Logging setup
configure_logging()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

DEFAULT_DRIVER = 'conf/driver.yaml'
DEFAULT_SCRAPER = 'conf/scraper.yaml'
DEFAULT_CREDENTIALS = 'conf/credentials.yaml'


def load_yaml(file_path):
    return yaml.load(open(file_path), Loader=yaml.SafeLoader)


if __name__ == '__main__':

    # Create argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', type=str, help='User to scrape data from')
    parser.add_argument('--credentials', default=DEFAULT_CREDENTIALS, help='Path to credentials')
    parser.add_argument('--scraper', default=DEFAULT_SCRAPER, help='Path to scraper config')
    parser.add_argument('--driver', default=DEFAULT_DRIVER, help='Path to driver config')

    # Parse arguments
    args = parser.parse_args()
    conf_scraper = load_yaml(args.scraper)
    conf_driver = load_yaml(args.driver)
    credentials = load_yaml(args.credentials)

    users = [args.user] if args.user else credentials['website'].keys()

    # Run scraper
    with run_driver(**conf_driver) as driver:
        for user in users:
            # Create credentials dict for this specific user (so that you do not embark all passwords)
            user_credentials = copy(credentials)
            user_credentials['website'] = user_credentials['website'][user]
            # Run the scraper
            LOGGER.info(f'Starting scraper for user {user}')
            scraper = VelibScraper(driver, user, user_credentials, **conf_scraper)
            scraper.run()

    LOGGER.info('Done scraping')
