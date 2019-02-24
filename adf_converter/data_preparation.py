import gzip
import shutil
import os
import tarfile
import pandas as pd
import hashlib

FEEDS_DIRECTORY = 'inputs'
HIT_DATA_HEADERS_FILE = 'inputs/lookup_data/column_headers.tsv'
LOOK_UP_DATA_DIRECTORY = 'lookup_data'

def get_file_name_without_path(file_path_or_name):

    return file_path_or_name.split('/')[-1]

def get_file_name_with_path(file_path_or_name, feeds_directory = FEEDS_DIRECTORY):

    return '/'.join([feeds_directory, file_path_or_name.split('/')[-1]])

def get_hit_data_column_headers(hit_data_headers = HIT_DATA_HEADERS_FILE):
    
    # The basic file that the feed headers are located. 

    if os.path.isfile(hit_data_headers):
        hit_data_headers_file = hit_data_headers
        with open(hit_data_headers_file) as file:
            line = file.readline()
            headers_list = line.replace('\n','').split('\t')
        return headers_list
    else:
        return None

def md5(file_name_with_path):

    # Get the MD5 for the file.
    
    hash_md5 = hashlib.md5()
    if os.path.isfile(file_name_with_path):
        with open(file_name_with_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    else:
        return None

def file_unzip(file_name):
    
    # Unzips a file and returns the name of the unzipped file.
    # Recieves as an argument the full file path.

    if '.gz' in file_name:
        file_name_unzipped = file_name.replace('.gz', '')
        with gzip.open(file_name, 'rb') as f_in:
            with open(file_name_unzipped, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return file_name_unzipped
    else:
        print 'File was not unzipped'
        return file_name

def file_zip(file_name):
        
    # Gzips a file and returns the name of the file.
    # Recieves as an argument the full file path.
    
    if '.gz' not in file_name:
        file_name_gziped = file_name + '.gz'
        with gzip.open(file_name_gziped, 'wb') as f_out:
            with open(file_name, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
    else:
        file_name_gziped = file_name

    return file_name_gziped

def get_available_files(path = FEEDS_DIRECTORY):
        
    feeds_directory = path
    
    lookup_file_flag = 'lookup_data.tar.gz'
    data_file_flag = 'tsv.gz'
    manifest_file_flag = '.txt'

    # Returns a list of data files and a lookup file that are ready to be uplaoded.
    # Data files are MD5-validated against the manifest files (optional)

    available_files = os.listdir(feeds_directory)
    lookup_file = ''
    data_files = []
    manifest_files = []
    returned_data_files = []
    for current_file in available_files:
        if lookup_file_flag in current_file:
            lookup_file = current_file
        elif data_file_flag in current_file:
            data_files.append(current_file)
        elif manifest_file_flag in current_file:
            manifest_files.append(current_file)


    for i in range(len(data_files)):
        data_files[i] = feeds_directory + '/' + data_files[i]

    returned_data_files = data_files

    return returned_data_files, lookup_file

def extract_and_get_lookup_file(lookup_file, destination_path = LOOK_UP_DATA_DIRECTORY):
    
    # Lookup file needs to be extracted from the tar file
    tar_lookup_file_with_path = get_file_name_with_path(lookup_file)
    
    if (os.path.isfile(tar_lookup_file_with_path)):
        lookup_files_with_path = extract_tar_file(tar_lookup_file_with_path, destination_path)
        return lookup_files_with_path
    else:
        print 'No lookup data available - Exiting'
        exit()

def extract_tar_file(tar_folder, destination_path):
    
    path_segment = destination_path
    data_directory = '/'.join([FEEDS_DIRECTORY,path_segment])

    # Unzip tarballs and extract the column headers file only.
    # tar_folder = tar_folder.replace('.gz', '')
    # assert tarfile.is_tarfile(tar_folder)
    tar_file = tarfile.open(tar_folder)
    # Extracts lookup data from the tar file
    tar_file.extractall(path = data_directory)

    extracted_files_with_path = []

    # Add the appropriate path to the lookup files
    for file_name in os.listdir(data_directory):
        if 'tsv' in file_name:
            extracted_files_with_path.append('/'.join([ FEEDS_DIRECTORY, path_segment, file_name]))

    return extracted_files_with_path

def parse_manifest_file(file_name):
        
    manifest_file = open(FEEDS_DIRECTORY + '/' + file_name, "rb")
    current_file_type = ''
    current_file_name = ''
    current_file_md5 = ''
    valid_data_files = []
    
    for line in manifest_file.readlines():
        line_parts = line.replace('\n','').split(': ')
        if ((line_parts[0] == 'Lookup-File') or (line_parts[0] == 'Data-File')):
            current_file_type = line_parts[0]
            current_file_name = line_parts[1]
        elif (line_parts[0] == 'MD5-Digest'):
            current_file_md5 = line_parts[1]
            # Validate only data files
            if (current_file_type == 'Data-File'):
                hash_value = md5(FEEDS_DIRECTORY + '/' + current_file_name)
                if (current_file_md5 != hash_value):
                    print 'Broken or missing file: {}'.format(current_file_name)
                else:
                    valid_data_files.append(current_file_name)

            current_file_type = ''
            current_file_name = ''
            current_file_md5 = ''
    
    manifest_file.close()
    return valid_data_files