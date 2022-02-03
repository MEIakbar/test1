import pandas
import os
import logging
import schedule
import time
from service.scrapping_data import get_data
from service.get_data import get_all_data

import warnings
warnings.filterwarnings("ignore")


def job(t):
    logging.info('Load Config... ')
    json_path = "./service/config.json"
    util = get_data.load_config_json(json_path)
    # scrapping
    util.get_data()
    # generate new data
    get_all_data()

# schedule.every().day.at("01:00").do(job,'It is 01:00')
schedule.every(10).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(60) # wait one minute