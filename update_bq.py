from google.oauth2 import service_account
from gspread_pandas import Spread
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import pandas_gbq
import logging, os, sys, re
import utils


FILE = ''
PROJECT_ID = ''
DATASET_ID = ''

# Logger
stdout_handler = logging.StreamHandler(stream=sys.stdout)
handlers = [stdout_handler]

logging.basicConfig(
    level=logging.DEBUG, 
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    handlers=handlers
)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# GS_Dipendenti
dipendenti = {
    'name': 'dipendenti',
    'spreadsheet_data': {
        "spreadsheet_id": '',
        "sheet_name": ''
    },
    'bq_destination': {
        "project_id": '',
        "destination_table": ''
    }
}

# GS_Economics
economics = {
    'name': 'economics',
    'spreadsheet_data': {
        "spreadsheet_id": '',
        "sheet_name": ''
    },
    'bq_destination': {
        "project_id": '',
        "destination_table": ''
    }
}


def delegate_credentials():
    # Delegated creds
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/spreadsheets.readonly',
              'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/bigquery', ]

    SERVICE_ACCOUNT_FILE = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    delegated_credentials = credentials.with_subject('')
    return delegated_credentials


def get_spread(spread_data, credentials):
    spread = Spread(spread_data['spreadsheet_id'], creds=credentials)

    df = spread.sheet_to_df(
        sheet=spread_data['sheet_name'], start_row=1).reset_index()

    return df


def column_names_normalize(df):
    for col_name in df:
        all_except_letters = re.sub(r"([?!^a-zA-Z]+)", "_", col_name)
        remove_chars_at_beginning = col_name.lstrip(all_except_letters)
        new_col_name = re.sub(r"[^0-9a-zA-Z]+", "_", remove_chars_at_beginning)
        df.rename(columns={col_name: new_col_name}, inplace=True)
    return df


def routine_gs(dict_name: dict, credentials, logger):
    logger.info("Getting data from Google Drive")
    # get data and convert it to pandas df
    df = get_spread(dict_name['spreadsheet_data'], credentials)

    logger.info("Removing not allowed characters")
    # remove chars not allowed in bq
    df = column_names_normalize(df)

    # upload to bq

    logger.info("Uploading to BigQuery")
    try:
        pandas_gbq.to_gbq(df, dict_name['bq_destination']['destination_table'],
            dict_name['bq_destination']['project_id'], if_exists='replace')
    except Exception as e:
        logger.error("Error while uploading")
        print(Exception, e)

    logger.info("Finished routine_gs:" + dict_name['name'])


def update_views(logger):
    logger.info("Start update views")

    SERVICE_ACCOUNT_FILE = os.environ['GOOGLE_APPLICATION_CREDENTIALS']

    bigquery_client = bigquery.Client(
        project=PROJECT_ID).from_service_account_json(SERVICE_ACCOUNT_FILE)
    dataset_ref = bigquery_client.dataset(DATASET_ID)

    views = utils.read_views(FILE)

    logger.info("Updating views")
    for view_name, view_query in views.items():
        create_view(bigquery_client, dataset_ref, view_name, view_query)

    logger.info("Finished update views")


def create_view(client, dataset, view_name, view_query):
    table_ref = dataset.table(view_name)

    # check if view already exists
    try:
        client.delete_table(table_ref)
    except NotFound:
        logger.info("Table view not present, nothing to delete")
    table = bigquery.Table(table_ref)
    table.view_query = view_query

    client.create_table(table)

    
if __name__ == '__main__':
    logger = logging.getLogger()

    credentials = delegate_credentials()
    
    # 
    routine_gs(dipendenti, credentials, logger)

    # 
    routine_gs(economics, credentials, logger)

    # update views
    update_views(logger)


