import pandas as pd
import utils as U
from datetime import datetime, timedelta
import numpy as np
import os
from google.cloud import storage
import data_ingestion


class ReportHandler(object):

    def __init__(
            self,
            config
    ):
        self.schema_size_data = None
        self.schema_date = None
        self.date_obj = None
        self.schema_size_file_name = None
        self.tmo_data = None
        self.tms_active = None
        self.pivot_license_data = None
        self.cfn_data = None
        self.cust_data = None
        self.tms_data = None
        self.license_data = None
        self.active_employee_data = None
        self.tms_file_name = None
        self.license_file_name = None
        self.active_employee_file_name = None
        self.date_str = None
        self.file_path = "../sample_data/"
        self.logger = U.set_logger(None, file_name="./report_handler.log")
        self.config = config
        self.get_date_string()
        self.generate_tms_file_name()
        self.generate_license_file_name()
        self.get_active_employee_file_name()
        self.get_active_employee_file_name()
        self.get_schema_size_file_name()

    @staticmethod
    def get_filtered_blob_list(file_prefix, report_folder_name = "TMSReports/"):
        filtered_blobs = []
        client = storage.Client()
        for blob in client.list_blobs(
                "cust01-heapdump",
                prefix=report_folder_name + file_prefix
        ):
            filtered_blobs.append(blob)
        return filtered_blobs

    def get_date_string(self):
        date_obj = datetime.utcnow()
        if 6 <= date_obj.hour < 18:
            self.config["variables"]["hour"] = 6
            print("between 6 and 18")
        else:
            print("after 18")
            self.config["variables"]["hour"] = 18
        self.date_obj = date_obj.replace(
#             day=self.config["variables"]["day"],
#             month=self.config["variables"]["month"],
            hour=self.config["variables"]["hour"]
        )            
        self.date_str = datetime.strftime(self.date_obj, '%Y%m%d%H')

    def generate_tms_file_name(self):
        self.tms_file_name = self.config["variables"]["tms_prefix"] + self.date_str + "_koss01" + ".csv"

    def generate_license_file_name(self):
        self.license_file_name = self.config["variables"]["license_prefix"] + self.date_str + "_koss01" + ".csv"

    def get_active_employee_file_name(self):
        self.active_employee_file_name = self.config["variables"]["active_employee_prefix"]

    def get_schema_size_file_name(self):
        self.schema_date = self.date_obj
        if self.config["variables"]["hour"] == 6:
            self.schema_date = self.schema_date - timedelta(days = 1)
            self.schema_date = self.schema_date.replace(
            # day=self.config["variables"]["day"],
#             month=self.config["variables"]["month"],
            hour = 18
        )
        self.schema_date_str = datetime.strftime(self.schema_date, '%Y%m%d%H')
        self.schema_size_file_name = self.config["variables"]["schema_size_prefix"] + self.schema_date_str + "_koss01" + ".csv"

    @staticmethod
    def get_csv_data(file_prefix, report_folder_name = "TMSReports/"):
        filtered_blobs = ReportHandler.get_filtered_blob_list(file_prefix, report_folder_name)
        dataframe = pd.DataFrame(data=None)
        for blob_name in filtered_blobs:
            dataframe = dataframe.append(pd.read_csv("gs://cust01-heapdump/{}".format(blob_name.name)))
            dataframe['source_name'] = blob_name.name
            print("data appended for {}".format(blob_name))
        print("dataframe creation completed")
        return dataframe
    
    def get_active_csv_data(self, file_prefix, report_folder_name = "TMSReports/"):
        filtered_blobs = ReportHandler.get_filtered_blob_list(file_prefix, report_folder_name)
        filtered_blobs = [blob for blob in filtered_blobs if self.date_str in blob.name]
        print(len(filtered_blobs))
        dataframe = pd.DataFrame(data=None)
        for blob_name in filtered_blobs:
            dataframe = dataframe.append(pd.read_csv("gs://cust01-heapdump/{}".format(blob_name.name)))
            dataframe['source_name'] = blob_name
            print("data appended for {}".format(blob_name))
        print("dataframe creation completed")
        return dataframe

    def transform_schema_size_data(self):
        self.schema_size_data = self.get_csv_data(self.schema_size_file_name, "ListSchemaSize/")
        self.schema_size_data = self.schema_size_data.rename(columns=self.config["variables"]["schema_size_col_mapping"])
        self.schema_size_data['stack'] = self.schema_size_data['server_name'].str.split('-').str[0]
        self.schema_size_data['env'] = self.schema_size_data['server_name'].str.split('-').str[1]
        self.schema_size_data['stack_env'] = self.schema_size_data['stack'] + "-" + self.schema_size_data['env']
        self.schema_size_data['schema_name'] = self.schema_size_data['schema_name'].str.lower()
        self.schema_size_data['stack_env'] = self.schema_size_data['stack_env'].str.lower()
        self.schema_size_data = self.schema_size_data.drop(
            ["SchemaSizeRaw", "database", "server_name", "stack", "env", "source_name"],
            axis=1
        )
        self.schema_size_data = self.schema_size_data.drop_duplicates(subset = ["stack_env","schema_name"])
        
    def transform_tms_data(self):
        self.tms_data = self.get_csv_data(self.tms_file_name)
        # self.tms_data = pd.read_csv(os.path.join(self.file_path, self.tms_file_name))
        self.tms_data = self.tms_data.rename(columns=self.config["variables"]["tms_col_mapping"])
        self.tms_data['customer_name'] = self.tms_data['tenant_name']
        self.tms_data['stack'] = self.tms_data['wfm_cluster_name'].str.split('-').str[0]
        self.tms_data['environment'] = self.tms_data['wfm_cluster_name'].str.split('-').str[1]
        self.tms_data['instance'] = self.tms_data['wfm_cluster_name'].str.split('-').str[2]
        self.tms_data['wfm_cluster'] = self.tms_data['wfm_cluster_name'].str.split('-').str[3]
        self.tms_data['stack_env'] = self.tms_data['stack'] + "-" + self.tms_data['environment']
        self.tms_data['schema_name'] = self.tms_data['schema_name'].str.lower()
        self.tms_data['stack_env'] = self.tms_data['stack_env'].str.lower()
        self.tms_data['customer_name'] = self.tms_data['customer_name'].str.lower()
        self.tms_data["company_code"] = self.tms_data["custom_tags"].str.split(";", ).str[1].str.split("-").str[1]
        self.tms_data['region'] = self.tms_data["custom_tags"].str.split(";", ).str[1].str.split("-").str[0]
        self.tms_data['client_move_or_flip'] = self.tms_data["custom_tags"].str.split(";", ).str[3]
        self.tms_data["hourly_timekeeping_entitlement"] = np.where(
            self.tms_data["hourly_timekeeping_entitlement"] > 9999999,
            9999999,
            self.tms_data["hourly_timekeeping_entitlement"]
        )
        self.tms_data["salaried_timekeeping_entitlement"] = np.where(
            self.tms_data["salaried_timekeeping_entitlement"] > 9999999,
            9999999,
            self.tms_data["salaried_timekeeping_entitlement"]
        )
        self.tms_data['total_entitlement_employee_count'] = self.tms_data["hourly_timekeeping_entitlement"] + self.tms_data["salaried_timekeeping_entitlement"]
        self.tms_data["total_entitlement_employee_count"] = np.where(
            self.tms_data["total_entitlement_employee_count"] > 9999999,
            9999999,
            self.tms_data["total_entitlement_employee_count"]
        )
        self.tms_data['non_prod_tenant_counts_less_500'] = self.tms_data["total_entitlement_employee_count"] - 500
        self.tms_data["non_prod_tenant_counts_less_500"] = np.where(
            self.tms_data["non_prod_tenant_counts_less_500"] <= 0,
            0,
            self.tms_data["non_prod_tenant_counts_less_500"]
        )
        self.tms_data = self.tms_data.drop_duplicates(
            subset=["customer_name", "schema_name", "stack_env"]
        ).reset_index(drop=True)

    def transform_license_data(self):
        self.license_data = self.get_csv_data(self.license_file_name)
#         self.license_data = pd.read_csv(os.path.join(self.file_path, self.license_file_name))
        self.license_data = self.license_data.rename(columns=self.config["variables"]["license_col_mapping"])
        self.license_data['stack'] = self.license_data['server_name'].str.split('-').str[0]
        self.license_data['env'] = self.license_data['server_name'].str.split('-').str[1]
        self.license_data['stack_env'] = self.license_data['stack'] + "-" + self.license_data['env']
        self.license_data['schema_name'] = self.license_data['schema_name'].str.lower()
        self.license_data['stack_env'] = self.license_data['stack_env'].str.lower()
        self.license_data['customer_name'] = self.license_data['customer_name'].str.lower()
        self.license_data['unique'] = self.license_data.index
        self.pivot_license_data = self.license_data.pivot(
            index=['customer_name', 'schema_name', 'unique', "stack_env"],
            columns='license_type',
            values='license_count'
        ).reset_index()
        self.pivot_license_data = self.pivot_license_data.rename(
            columns=self.config["variables"]["pivot_license_mapping"]
        )
        self.pivot_license_data = self.pivot_license_data.drop("unique", axis=1)
        self.pivot_license_data = self.pivot_license_data.drop_duplicates(
            subset=["customer_name", "schema_name", "stack_env"]
        ).reset_index(drop=True)
        cols = list(self.pivot_license_data.columns)
        cols = [x.lower() for x in cols]
        self.pivot_license_data.columns = cols

    def get_active_employee_data(self):
        # TODO adhoc function for file reading in local not required in gcp
        file_list = os.listdir(self.file_path)
        active_employee_files = [x for x in file_list if "Active" in x and self.date_str in x]
        data = pd.DataFrame(data=None)
        for file in active_employee_files:
            data = data.append(pd.read_csv(os.path.join(self.file_path, file)))
        data = data.reset_index(drop=True)
        return data

    def transform_active_employee_data(self):
#         self.active_employee_data = self.get_csv_data(self.active_employee_file_name, end_offset=self.date_str)
        self.active_employee_data = self.get_active_csv_data(self.active_employee_file_name)
#         self.active_employee_data = self.get_active_employee_data() 
        self.active_employee_data = self.active_employee_data.rename(
            columns=self.config["variables"]["active_employee_col_mapping"]
        )
        self.active_employee_data['stack'] = self.active_employee_data['server_name'].str.split('-').str[0]
        self.active_employee_data['env'] = self.active_employee_data['server_name'].str.split('-').str[1]
        self.active_employee_data['stack_env'] = self.active_employee_data['stack'] + "-" + self.active_employee_data[
            'env']
        self.active_employee_data['schema_name'] = self.active_employee_data['schema_name'].str.lower()
        self.active_employee_data['stack_env'] = self.active_employee_data['stack_env'].str.lower()
        self.active_employee_data['customer_name'] = self.active_employee_data['customer_name'].str.lower()
        self.active_employee_data = self.active_employee_data.drop_duplicates(
            subset=["customer_name", "schema_name", "stack_env"]
        ).reset_index(drop=True)

    def transform_tmo_data(self):
        self.transform_tms_data()
        self.transform_active_employee_data()
        self.transform_license_data()
        self.transform_schema_size_data()
        self.tms_active = pd.merge(
            self.tms_data,
            self.active_employee_data.drop(["server_name", "customer_name", "stack", "env"], axis=1),
            on=["schema_name", "stack_env"],
            how="left"
        )
        self.tmo_data = pd.merge(
            self.tms_active,
            self.pivot_license_data.drop(["customer_name"], axis=1),
            on=["schema_name", "stack_env"],
            how="left"
        )
        self.tmo_data['total_usage_employee_count'] = self.tmo_data["core_hourly"] + self.tmo_data["core_salary"]
        self.tmo_data['execution_time'] = datetime.utcnow()
        self.tmo_data['source_name'] = "tms_wfd_db_" + self.date_str

        self.tmo_data = pd.merge(
            self.tmo_data,
            self.schema_size_data,
            on=["schema_name", "stack_env"],
            how="left"
        )

    def transform_cfn_cust_data(self):
        self.tmo_data = self.tmo_data[self.config['destination_database']['col_list']]
#         self.cfn_data = self.tmo_data[self.tmo_data['stack'] == 'kcfn01']
#         self.cust_data = self.tmo_data[self.tmo_data['stack'] != 'kcfn01']
        self.cfn_data = self.tmo_data[self.tmo_data['stack'].str.contains('cfn', case =False)]
        self.cust_data = self.tmo_data[self.tmo_data['stack'].str.contains('cus', case =False)]

    def ingest_cfn_data(self):
        di_obj = data_ingestion.DataIngestion(self.config, self.cfn_data)
        di_obj.delete_all_records(self.config['destination_database']['cfn_table_id'])
        di_obj.run_data_ingestion(self.config['destination_database']['cfn_table_id'])

    def ingest_cust_data(self):
        di_obj = data_ingestion.DataIngestion(self.config, dataframe=None)
        list_df = [self.cust_data[i:i+self.config["destination_database"]["chunk_size"]] for i in range(0,len(self.cust_data),self.config["destination_database"]["chunk_size"])]
        print(len(list_df))
        di_obj.delete_all_records(self.config['destination_database']['cust_table_id'])
        for count, data in enumerate(list_df):
            di_obj = data_ingestion.DataIngestion(self.config, data)
            di_obj.run_data_ingestion(self.config['destination_database']['cust_table_id'])
            self.logger.info("data inserted for chunk {}".format(count))
#         di_obj.run_data_ingestion(self.config['destination_database']['cust_table_id'])

    def run_report_handler(self):
        self.transform_tmo_data()
        self.transform_cfn_cust_data()
        self.ingest_cfn_data()
        self.ingest_cust_data()
