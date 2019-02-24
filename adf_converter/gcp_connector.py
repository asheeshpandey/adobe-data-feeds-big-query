from google.cloud import storage
from google.cloud import bigquery
from google.cloud.storage import Blob
import os
import re

class google_gloud_platform():
    
    def __init__(self, configuration, feeds_directory = 'inputs'):
        
        self.project_id = configuration['project_name']
        
        # BigQuery settings
        self.client = bigquery.Client.from_service_account_json( configuration['service_account_file'] )
        self.dataset_ref = self.client.dataset(configuration['dataset_id'])
        
        # BigQuery Job Configuration
        self.job_config = bigquery.LoadJobConfig()
        self.job_config.allow_jagged_rows = True
        self.job_config.ignore_unknown_values = True
        self.job_config.field_delimiter = '\t'
        self.job_config.encoding = 'ISO-8859-1'
        self.job_config.null_marker = ''
        self.job_config.skip_leading_rows = 0
        self.job_config.quote_character = '^'
        # The source format defaults to CSV, so the line below is optional.
        self.job_config.source_format = bigquery.SourceFormat.CSV

        # Flag for WRITE_TRUNCATE or WRITE_APPEND to be used for hits
        self.hit_data_table_overwrite = configuration['hit_data_overwrite']

        self.hit_data_table_name = configuration['table_name']

        # Cloud Storage Settings
        self.bucket_name = configuration['bucket_name']
        self.bucket_feeds_path = 'inputs/hit_data'

        self.staging_path = configuration['staging_path']
        self.loading_path = configuration['loading_path']
        self.archiving_path = configuration['archiving_path']

        self.storage_client = storage.Client.from_service_account_json( configuration['service_account_file'] )
        self.bucket = self.storage_client.get_bucket(self.bucket_name)

        self.feeds_directory = feeds_directory
        
    def prepare_and_load(self, blob_path, headers, destination_table, custom_schema = None):
        
        schema_fields = []

        headers_regex_pattern = '[^a-zA-Z0-9_]'
        regex = re.compile(headers_regex_pattern)
        # From BQ Error messages:
        # Fields must contain only letters, numbers, and underscores, 
        # start with a letter or underscore, and be at most 128 characters long
        for header in headers: 
            column_name = regex.sub('',header)
            column_type = 'STRING'

            if (custom_schema is not None) and (header in custom_schema):
                column_type = custom_schema[header]
        
            if ((header == 'date_time') and (column_type == 'TIMESTAMP')):
                partitioning = bigquery.table.TimePartitioning(field='date_time', require_partition_filter = True)
                self.job_config.time_partitioning = partitioning

            column = bigquery.SchemaField(column_name, column_type)
            schema_fields.append(column)

        self.job_config.schema = schema_fields
        self.load(blob_path, destination_table)

    def load(self, blob_path, destination_table):

        # Save data into BigQuery
        uri = blob_path

        load_job = self.client.load_table_from_uri(uri,\
            self.dataset_ref.table(destination_table),\
            job_config = self.job_config)  # API request
        print('Starting job {}'.format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        print('Job finished.')

    def upload_multiple_files_to_storage(self, files_list = None):
        
        # List of blob names uplaoded. This is used subsequently to load the tables.
        blob_names = []

        for current_file in files_list:
            blob_names.append(self.upload_file_to_storage(self.bucket, current_file))
        return blob_names
        
    def upload_file_to_storage(self, bucket, file_name):
        
        blob_name = file_name
        
        # Check if the file is hit data or lookup data
        path_segments = blob_name.split('/')
        if len(path_segments) == 2:
            path_segments.insert(1, 'hit_data')
        blob_name = '/'.join(path_segments)

        if (file_name.find('tsv')>0):
            print 'Start uploading: ' + file_name
            # Filter only files with hit-level data
            blob = bucket.blob(blob_name)
            # blob.content_encoding = 'gzip'
            blob.upload_from_filename(file_name, content_type='application/gzip')
            print 'File uploaded: ' + blob_name

        # Return the final blob name uploaded.
        return blob_name

    def load_hit_data(self, hit_data_headers, custom_schema = None):

        if (hit_data_headers is not None):

            if self.hit_data_table_overwrite == 'true':
                self.job_config.write_disposition = 'WRITE_TRUNCATE'
            else:
                self.job_config.write_disposition = 'WRITE_APPEND'

            hit_data_blob = 'gs://' + str(self.bucket_name) + '/'  + str(self.archiving_path) + '/*.tsv.gz'
            
            self.prepare_and_load(blob_path = hit_data_blob, headers = hit_data_headers, \
                    destination_table = self.hit_data_table_name, custom_schema = custom_schema)
        else:
            raise ValueError('Missing hit data headers.')

    def load_lookup_data(self, lookup_blob_names, custom_schema = None):
    
        for blob_name in lookup_blob_names:

            table_name = blob_name.split('/')[-1].replace('.tsv','').replace('.gz','')
            lookup_headers = ['key','value']

            # Set time partitioning to None for Lookup data. It is not applicable.
            self.job_config.time_partitioning = None
            
            # Overwrite lookup data
            self.job_config.write_disposition = 'WRITE_TRUNCATE'

            full_blob_path = 'gs://' + str(self.bucket_name) + '/'  + str(blob_name)
            self.prepare_and_load(blob_path = full_blob_path, headers = lookup_headers, destination_table = table_name, custom_schema = custom_schema)

    def list_blobs_with_prefix(self, prefix, delimiter=None):
        """Lists all the blobs in the bucket that begin with the prefix.
        This can be used to list all blobs in a "folder", e.g. "public/".
        The delimiter argument can be used to restrict the results to only the
        "files" in the given "folder". Without the delimiter, the entire tree under
        the prefix is returned. For example, given these blobs.
        """
        
        bucket = self.bucket
        blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

        names = []
        for blob in blobs:
            names.append(blob.name)

        if delimiter:
            print('Prefixes:')
            for prefix in blobs.prefixes:
                print(prefix)

        return names

    def archive_blob(self, blob_name):
        """Renames a blob."""

        new_name = blob_name.replace(self.staging_path, self.archiving_path)
        bucket = self.bucket
        blob = bucket.blob(blob_name)

        new_blob = bucket.rename_blob(blob, new_name)

        # print('Blob {} has been renamed to {}'.format(blob.name, new_blob.name))

    def prepare_blobs_for_loading_archiving(self):
        
        blob_names = self.list_blobs_with_prefix(self.staging_path)
        
        for blob_name in blob_names:
            self.copy_blob_to_loading(blob_name)
            self.archive_blob(blob_name)

    def copy_blob_to_loading(self, blob_name):
        """Copies a blob from one bucket to another with a new name."""
        print blob_name
        source_blob = self.bucket.blob(blob_name)
        
        source_bucket = self.bucket
        destination_bucket = self.bucket

        new_blob_name = blob_name.replace(self.staging_path,self.loading_path)
        
        new_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, new_blob_name)

        print('Blob {} copied to blob {}.'.format(
            source_blob.name, new_blob.name ))

    def delete_loaded_blobs(self):
      
        blob_names = self.list_blobs_with_prefix(self.loading_path)

        for blob_name in blob_names:
            bucket = self.bucket
            blob = bucket.blob(blob_name)
            blob.delete()
            print('Blob {} deleted.'.format(blob_name))