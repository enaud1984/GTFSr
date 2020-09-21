import configparser
import datetime
import logging
import os
from apscheduler.schedulers.background import BlockingScheduler
import paramiko
import shutil

from GTFSr.gtfsrdb import GTFSrDB
from utils import Utils

sched = BlockingScheduler()  # background se contemporaneamente fa altro
# notice that the "start to watch" process happen here
logger = logging.getLogger( __name__ )


class DownloadPB( Utils ):
    def __init__(self):
        self.getConf( "config.ini", section=["MANAGE FILE"] )
        if not os.path.isdir( self.pb_file ):
            os.mkdir( self.pb_file )
        #sched.add_job( self.downloadFile, 'interval', minutes=int(self.timeloop) )
        #sched.add_job( self.downloadFile, 'interval', seconds=6 )
        #sched.start()
        #self.downloadFile()

        gtfsR=GTFSrDB(create=True)


    def downloadFile(self):
        self.moveFile()
        logger.info( "Download files in pbFile Folder" )
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
        i = 0
        listFile = [self.alerts_feed, self.position_feed, self.trip_feed]
        try:
            ssh_client.connect( hostname=self.hostname_data_lake, username=self.username, password=self.password )
            for file in listFile:
                ftp_client = ssh_client.open_sftp()
                ftp_client.get( file, self.pb_file+"\\"+file.split( "/" )[-1] )
                ftp_client.close()
            ssh_client.close()
            i += 1
        except Exception as e:
            logger.error( "ERRORE downloadFile, hostname: {} file: {} ".format(self.hostname_data_lake,file),e )

    def moveFile(self):
        logger.info("Move files in pbFileBackup Folder")
        if not os.path.isdir( self.pb_file_backup ):
            os.mkdir( self.pb_file_backup )
        for name in os.listdir( self.pb_file ):
            src = os.path.join( self.pb_file, name )
            dest = os.path.join( self.pb_file_backup, name + "_" + datetime.datetime.now().strftime( "%Y%m%d_%H%M%S" ) )
            shutil.move( src, dest )


class GTFSs:  # file statici .txt usare import pygtfs
    pass

# class GTFSr( GTFSr.GTFSrDB ): #file realtime .pb

if not os.path.isdir( "log" ):
    os.mkdir( "log" )

logging.basicConfig( filename="log/logGTFSrLoad_{}.log".format(
    datetime.datetime.strftime( datetime.datetime.now(), "%Y%m%d_%H%M%S_%f"[:-3] ) ),
                     format="%(asctime)s - %(levelname)s:%(message)s",
                     level=logging.INFO,
                     datefmt='%d/%m/%Y %H:%M:%S' )

obj = DownloadPB()
