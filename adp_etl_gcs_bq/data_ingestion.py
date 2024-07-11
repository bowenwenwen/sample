import json
from google.oauth2 import service_account
from collections import OrderedDict
from google.cloud import bigquery
import numpy as np
from datetime import timedelta
import utils as U


class DataIngestion(object):

    def __init__(
            self,
            config,
            dataframe
    ):
        self.json_data = None
        self.credentials = None
        self.config = config
        self.logger = U.set_logger(None, file_name="./data_ingestion.log")
        self.dataframe = dataframe
        self.get_credentials()
        self.bigquery_credentials = service_account.Credentials.from_service_account_info(self.credentials)
        self.bq_client = bigquery.Client(
            project=self.config["destination_database"]["project_id"],
            credentials=self.bigquery_credentials
        )

    def get_credentials(self):
        self.logger.info("credential read up done")
        with open(self.config["destination_database"]["service_account_path"]) as source:
            self.credentials = json.load(source)

    def list_datasets(self):
        datasets = list(self.bq_client.list_datasets())  # Make an API request.
        project = self.bq_client.project
        if datasets:
            self.logger.info("Datasets in project {}:".format(project))
            for dataset in datasets:
                self.logger.info("\t{}".format(dataset.dataset_id))
        else:
            self.logger.info("{} project does not contain any datasets.".format(project))

    def get_dataset(self):
        dataset = self.bq_client.get_dataset(self.config["destination_database"]["dataset_id"])  # Make an API request.
        full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
        friendly_name = dataset.friendly_name
        self.logger.info(
            "Got dataset '{}' with friendly_name '{}'.".format(
                full_dataset_id, friendly_name
            )
        )

    def update_dataset(self, new_description):
        dataset = self.bq_client.get_dataset(self.config["destination_database"]["dataset_id"])
        dataset.description = new_description
        dataset = self.bq_client.update_dataset(dataset, ["description"])
        full_dataset_id = "{}.{}".format(dataset.project, dataset.dataset_id)
        self.logger.info(
            "Updated dataset '{}' with description '{}'.".format(
                full_dataset_id, dataset.description
            )
        )

    def delete_dataset(self):
        self.bq_client.delete_dataset(
            self.config["destination_database"]["dataset_id"],
            delete_contents=True, not_found_ok=True
        )
        self.logger.info("Deleted dataset '{}'.".format(self.config["destination_database"]["dataset_id"]))

    def delete_table(self, table_id):
        self.bq_client.delete_table(self.config["destination_database"]["table_id"], not_found_ok=True)
        self.logger.info("Deleted table '{}'.".format(self.config["destination_database"]["table_id"]))

    def list_tables(self):
        tables = self.bq_client.list_tables(self.config["destination_database"]["dataset_id"])
        self.logger.info("Tables contained in '{}':".format(self.config["destination_database"]["dataset_id"]))
        for table in tables:
            self.logger.info("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def query_datasets(self, table_id):
        QUERY = "SELECT max(id) as max_id FROM `{}` ".format(table_id)
        query_job = self.bq_client.query(QUERY)
        rows = query_job.result()
        for row in rows:
            max_id_val = row.max_id
        return max_id_val
    
    def delete_all_records(self, table_id):
        QUERY = "DELETE FROM `{}` WHERE true;".format(table_id)
        query_job = self.bq_client.query(QUERY)
    
    def update_inactive_records(self, table_id):
        QUERY = " UPDATE `{}` SET isactive = 0 where true".format(table_id)
        query_job = self.bq_client.query(QUERY)
    
    def insert_rows(self, table_id):
        errors = self.bq_client.insert_rows_json(
            table_id,
            self.json_data
        )
        if not errors:
            self.logger.info("New rows have been added to table id {}".format(
                table_id
                )
            )
        else:
            self.logger.info("Encountered errors while inserting rows: {}".format(errors))

    def run_data_ingestion(self, table_id):
        self.json_data = self.dataframe.to_json(orient='records', date_format='iso')
        self.json_data = json.loads(self.json_data, object_pairs_hook=OrderedDict)
        self.logger.info("data packet created for insertion")
        self.insert_rows(table_id)
        self.logger.info("data appended into bigquery table")
