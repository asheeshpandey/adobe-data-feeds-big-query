from adf_converter.data_preparation import exporter
from adf_converter.data_transfer import ftp_downloader
from adf_converter.gcp_connector import google_gloud_platform

'''
FTP Transfer Details
'''

ftp_configuration = {
    'fpt_host' : '[FTP Host]',
    'ftp_username' : '[FTP Username]',
    'ftp_password' : '[FTP Passwordd]',
    'ftp_path' : '[FTP Path or /]'
}

'''
Google Cloud Configuration Example
'''
configuration_gcp = {
    'project_name' : '[project id]',
    
    # BigQuery Configuration
    'dataset_id' : '[dataset id]', 
    'table_name' : '[table name]',
    
    # Cloud Storage Configuration
    'bucket_name' : '[bucket name]',
    
    'staging_path' : 'inputs/hit_data',
    'loading_path' : 'inputs/pending_loading',
    'archiving_path' : 'inputs/archive'
}

# Define optional schema
# https://cloud.google.com/bigquery/docs/schemas#standard_sql_data_types
configuration_gcp_schema = {
    # Example to transform UNIX timestamp into date:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#date_from_unix_date
    'hit_time_gmt' : 'INT64' 
}

'''
BigQuery - Google Cloud Data Platform Example
1) Data is downloaded from FTP
2) Data files are uploaded to GCP Storage
3) BQ Load job is created
4) Data is cleaned up from FTP,local file system and Storage.
'''

# Download data
ftp = ftp_downloader(ftp_configuration = ftp_configuration)

# Initiate data loading to BQ
data_exporter = exporter()
# Lookup file needs to be extracted so then we can create the BQ schema
data_files_gzips, lookup_file_folder = data_exporter.get_available_files(validate_checksum = False, sanitize_characters = False)
lookup_files_with_path = data_exporter.extract_and_get_lookup_file(lookup_file_folder, aszipped = False)

gcp = google_gloud_platform(configuration_gcp)
gcp.upload_multiple_files_to_storage(files_list = data_files_gzips)

lookup_files_blob_names = gcp.upload_multiple_files_to_storage(files_list = lookup_files_with_path)
hit_data_headers = data_exporter.get_hit_data_column_headers()

# Archive loaded files sanitized files get renamed to loaded.
gcp.prepare_blobs_for_loading_archiving()

gcp.load_lookup_data(lookup_blob_names = lookup_files_blob_names)
gcp.load_hit_data(hit_data_headers = hit_data_headers, custom_schema= configuration_gcp_schema)
gcp.delete_loaded_blobs()

ftp.clean_data_from_ftp_v2()
ftp.clean_data_from_local()