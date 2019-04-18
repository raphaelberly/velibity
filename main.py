import argparse
import logging

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


if __name__ == '__main__':

    # Create argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', required=True, type=str, help='User to scrape data from')
    parser.add_argument('--credentials', default=DEFAULT_CREDENTIALS, help='Path to credentials')
    parser.add_argument('--scraper', default=DEFAULT_SCRAPER, help='Path to scraper config')
    parser.add_argument('--driver', default=DEFAULT_DRIVER, help='Path to driver config')

    # Parse arguments
    args = parser.parse_args()
    conf_scraper = yaml.load(open(args.scraper), Loader=yaml.SafeLoader)
    conf_driver = yaml.load(open(args.driver), Loader=yaml.SafeLoader)
    credentials = yaml.load(open(args.credentials), Loader=yaml.SafeLoader)
    try:
        credentials = credentials[args.user]
    except IndexError:
        LOGGER.error(f'User {args.user} not found in {args.credentials}')

    LOGGER.info(f'Starting scraper for user {args.user}')

    # Run scraper
    with run_driver(**conf_driver) as driver:
        scraper = VelibScraper(driver=driver, credentials=credentials, **conf_scraper)
        scraper.login()
        # scraper.run()

    LOGGER.info('Done scraping')
