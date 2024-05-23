# Import necessary libraries
import json
import csv
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


# Data Ingestion
class DataIngestion:
    def ingest_data(self, data, data_format):
        if data_format == 'json':
            return self.ingest_json(data)
        elif data_format == 'csv':
            return self.ingest_csv(data)
        elif data_format == 'avro':
            return self.ingest_avro(data)

    def ingest_json(self, data):
        # Ingest JSON data
        return json.loads(data)

    def ingest_csv(self, data):
        # Ingest CSV data
        return csv.reader(data)

    def ingest_avro(self, data):
        # Ingest Avro data
        reader = DataFileReader(open(data, "rb"), DatumReader())
        avro_data = [datum for datum in reader]
        reader.close()
        return avro_data


# Data Processing
class DataProcessing:
    def process_data(self, df_impressions, df_clicks):
        # Assuming impression & click dataframes (df_impressions & df_clicks) are prepared
        df_joined = df_impressions.merge(df_clicks, on=['user_id', 'ad_id'], how='left')
        df_joined['ctr'] = df_joined['click_id'].fillna(0) / df_joined.shape[0]  # Calculate CTR
        return df_joined['ctr']


# Data Storage
class DataStorage:
    def store_data(self, data):
        # Implement your data storage logic here
        pass
