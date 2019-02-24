# Import dependencies for the FTP/Transfer manager
import pysftp
import os
import shutil

class ftp_downloader():
    
    def __init__(self, ftp_configuration, local_feeds_path = 'inputs'):
        # Ignore certificate errors
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys = None 

        # Local file that feeds are stored
        self.feeds_local_folder = local_feeds_path

        # Local file that temporary output files are stored
        self.feeds_remote_path = ftp_configuration['ftp_path']

        self.host = ftp_configuration['fpt_host']
        self.username = ftp_configuration['ftp_username']
        self.password = ftp_configuration['ftp_password']

        # Check if FTP contains any data. If not, drop the rest of the execution
        self.ftp_connection = pysftp.Connection(host = self.host, username = self.username,\
                                password = self.password, cnopts = self.cnopts)

        self.ftp_connection.cwd(self.feeds_remote_path)
        list_of_new_files = self.ftp_connection.listdir()
        if len(list_of_new_files) == 0:
            print 'No new files available for download. Exiting...'
            exit()
        self.ftp_connection.cwd('/')

        if not os.path.exists(self.feeds_local_folder):
            os.mkdir(self.feeds_local_folder)
        
    def download_data_from_ftp(self):

        self.ftp_connection.get_d(remotedir = self.feeds_remote_path, localdir = self.feeds_local_folder)
        
    def clean_data_from_local(self):

        print 'Local File Clean-up'
        shutil.rmtree(self.feeds_local_folder)

    def clean_data_from_ftp_v2(self):

        print('Cleaning custom dir')
        files = self.ftp_connection.listdir(self.feeds_remote_path)
        for file in files:
            self.ftp_connection.remove(self.feeds_remote_path + '/' + file)
            print('Deleted file: ' + str(file))