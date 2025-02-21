import requests
import pandas as pd
import time
import config.logger as logger
import argparse
import os
import json


SOURCE_URL = os.environ['DATA_URL']

logger = logger.setup_logger()


def main(sleep_time: float, max_record_count: int) -> None:

    df = pd.read_parquet(SOURCE_URL)

    if max_record_count == 0:
        max_record_count = len(df)

    record_counter = 0

    top_level_fields = {'event_type': 'YellowTaxiTripRecords',
                        'schema_version': '0-0-1'}

    for index, row in df.iterrows():

        try:
            json_header = {'Content-Type': 'application/json'}
            url = 'http://collector/api/v1/collect'
            row_data = row.to_dict()
            row_data['tpep_pickup_datetime'] = row_data['tpep_pickup_datetime'].strftime('%Y-%m-%d %H:%M:%S')
            row_data['tpep_dropoff_datetime'] = row_data['tpep_dropoff_datetime'].strftime('%Y-%m-%d %H:%M:%S')
            processed_event = {**top_level_fields, "payload": row_data}
            logger.info(requests.post(url, data=json.dumps(processed_event), headers=json_header))
        except:
            logger.error(f'Failed to process payload')

        record_counter += 1
        time.sleep(sleep_time)

        if record_counter >= max_record_count:
            break


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Runs producer which writes TLCTripRecordData to collector endpoint")
    parser.add_argument('-s', '--sleep_time', required=False, default='1')
    parser.add_argument('-m', '--max_record_count', required=False, default='1000')
    args = parser.parse_args()

    logger.info(f'Starting sample producer (TLCTripRecordData) with {args.sleep_time} s between sending records')

    main(float(args.sleep_time), int(args.max_record_count))
