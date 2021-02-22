import os
import time
import sys
import yaml
import csv
import json
import logging
import logging.config
from pathlib import Path
import pandas as pd
from sodapy import Socrata
import snowflake.connector as snow


def env_setup():

    logging_cfg_file = Path(__file__).parent / 'config' / 'logging.yaml'
    app_cfg_file = Path(__file__).parent / 'config' / 'app.yaml'

    output_dir = Path(os.getcwd()) / "output"
    output_file = output_dir / "tlc_applications.csv"
    status_file = output_dir / "status"
    
    if not output_dir.exists():
        output_dir.mkdir()

    with open(logging_cfg_file, 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    with open(app_cfg_file, 'r') as f:
        app_config = yaml.safe_load(f.read())

    app_config['output_file'] = output_file
    app_config['status_file'] = status_file
    
    return app_config


def get_data(datasource, status_file):

    logger = logging.getLogger(__name__)

    domain = datasource['domain']
    datasets = datasource['datasets']
    limit = datasource['limit']

    client = Socrata(domain, app_token=None)
    data = []

    if status_file.exists():
        with open(status_file) as f:
            latest_sync = json.load(f)['latest_sync']
    else:
        latest_sync = "1970-01-01T00:00:00.000"

    now = time.strftime("%Y-%m-%dT%H:%M:%S.000", time.gmtime())

    for ds in datasets:
        logger.info(f"Get dataset {ds} from domain '{domain}'")

        api_fieldname = 'lastupdate' if ds == 'dpec-ucu7' else 'last_updated'

        results = client.get(ds, limit=limit, where=f"{api_fieldname} >= '{latest_sync}'")

        for item in results:
            #Sync filed names
            if ds == 'dpec-ucu7':
                item['last_updated'] = item.pop('lastupdate')

            """
            app_no 5735329 and 5935513 are found in both datasets
            keeping source identifier to properly handle ongoing updates
            """
            item.update({"source_id": ds})

        data += results

    with open(status_file, 'w') as f:
        json.dump({"latest_sync": now}, f)

    if not data:
        logger.info(f'No updates since latest sync at {latest_sync}')
        sys.exit(0)

    return data


def build_snapshot(data, output_file):

    logger = logging.getLogger(__name__)
    logger.info('Building snapshot...')

    df_new = pd.DataFrame.from_dict(data)

    if output_file.exists():
        #Load previously generated snapshot
        df_prev = pd.read_csv(output_file, sep=',')

        df_new['match_key'] = df_new['app_no'].astype(str) + df_new['source_id']
        df_prev['match_key'] = df_prev['app_no'].astype(str) + df_prev['source_id']
    
        df_prev = df_prev[~df_prev['match_key'].isin(df_new['match_key'])]
        
        df_new = pd.concat([df_prev, df_new])
    
        df_new = df_new.drop(columns=['match_key'])

    df_new.to_csv(output_file, sep=",", index=False, quoting=csv.QUOTE_ALL)

    logger.info('Snapshot output/tlc_applications.csv created')


def load_to_db(snowflake, output_file):

    logger = logging.getLogger(__name__)
    logger.info('Loading snapshot to database')

    conn = snow.connect(
                user=os.getenv("SNOWFLAKE_USR"),
                password=os.getenv("SNOWFLAKE_PWD"),
                account=snowflake['account'],
                warehouse=snowflake['warehouse'],
                database=snowflake['database'],
                schema=snowflake['schema']
                )

    cur = conn.cursor()

    cur.execute("DELETE FROM tlc_applications")
    cur.execute(f"PUT file://{output_file} @%tlc_applications OVERWRITE = TRUE")
    cur.execute("COPY INTO tlc_applications file_format = (type = csv SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')")

    cur.close()
    conn.close()

    logger.info('Loading done')
