import os
import pandas as pd
import numpy as np
import datetime
from ftplib import FTP

def down_kp(timeini, timeend, dataPath, downloadData, filename = 'none'):

    potsdam = downloadData # if 1, download the kp data from potsdam, if 0 you must import manually from spidr
    timeini = timeini
    timeend = timeend

    # the filename is used only when the data is from SPIDR repository
    ###

    # directory of the data
    dataDownlDir = dataPath


    str_temp = 'Kp_ap_{:}.txt'.format(int(timeini.year))
    print(str_temp)
    if os.path.isfile(dataDownlDir + str_temp):
        print(f"{str_temp} already at: {dataDownlDir}")
        downloadData = 0
    else:
        print(f"Downloading {str_temp}")
        downloadData = 1

    if downloadData == 1:
        # Download data

        # define the directory and host in the ftp
        host = 'ftp.gfz-potsdam.de'
        working_directory = '/pub/home/obs/Kp_ap_Ap_SN_F107/'
        #### download of the data #################
        mx = Downloader(host)
        # Connecting
        mx.connect()
        # Set downloaded data directory
        mx.set_output_directory(dataDownlDir)
        # Set FTP directory to download
        mx.set_directory(working_directory)
        # Download single data
        mx.download_one_data(str_temp)

    year, mm, dd, hh, kp = np.loadtxt(dataDownlDir + str_temp,
                                      usecols=(0, 1, 2, 3, 7), unpack=True)

    time = list()
    for ii in range(len(year)):
        t_string = f"{int(year[ii])}-{int(mm[ii])}-{int(dd[ii])}:{int(hh[ii])}"
        time.append(datetime.datetime.strptime(t_string, '%Y-%m-%d:%H'))

    time = pd.to_datetime(time)

    df_kp = pd.DataFrame(kp, index=time, columns=['Kp'])

    return df_kp



#########################################################################################

class Downloader():
    def __init__(self, hostname, user='', passwd=''):
        # Parametros
        self.host = hostname
        self.user = user
        self.passwd = passwd
        self.directory = None
        self.ftp = None
        self.output = None

    def set_user_and_password(self, user, passwd):
        self.user = user
        self.passwd = passwd

    def set_output_directory(self, output):
        self.output = str(output)

    def connect(self):
        try:
            self.ftp = FTP(str(self.host), user=str(self.user), passwd=str(self.passwd))
            self.ftp.login()
            print('Connected to: ' + str(self.host))
        except (Exception) as e:
            print(e)

    def set_directory(self, directory):
        try:
            self.ftp.cwd(directory)
            print('..')
        except (Exception) as e:
            print('Failed to set directory.\n' + str(e))

    def download_one_data(self, filename):
        try:
            self.ftp.retrbinary(str('RETR ' + filename), open(self.output + filename, 'wb').write)
            print("Downloaded: " + str(filename))
        except (Exception) as e:
            print(e)

    def download_many_data(self, filename_list):
        try:
            for filename in filename_list:
                self.ftp.retrbinary(str('RETR ' + filename), open(self.output + filename, 'wb').write)
                print('Downloaded: ' + str(filename))
        except (Exception) as e:
            raise e

    def close(self):
        self.ftp.close()