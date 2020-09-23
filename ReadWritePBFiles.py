import configparser
import datetime
import logging
import os
# from apscheduler.schedulers.background import BlockingScheduler
import paramiko
import shutil

from pyspark.sql import SparkSession

from gtfsrdb import GTFSrDB
from utils import Utils

# sched = BlockingScheduler()  # background se contemporaneamente fa altro
# notice that the "start to watch" process happen here
logger = logging.getLogger( __name__ )


class ReadWritePBFile( Utils ):
    def __init__(self):
        spark = SparkSession.builder.appName( "Python Spark SQL" ).getOrCreate()
        spark.stop()
        self.getConf( "config.ini", section=["MANAGE FILE"] )
        if not os.path.isdir( self.pb_file ):
            os.mkdir( self.pb_file )
        self.logname = "logGTFSrLoad_{}.log".format(
            datetime.datetime.strftime( datetime.datetime.now(), "%Y%m%d_%H%M%S_%f"[:-3] ) )
        self.logpath = "/tmp/{}".format( self.logname )
        logging.basicConfig( filename=self.logpath, format="%(asctime)s - %(levelname)s:%(message)s",
                             level=logging.INFO, datefmt='%d/%m/%Y %H:%M:%S' )
        # sched.add_job( self.downloadFile, 'interval', minutes=int(self.timeloop) )
        # sched.add_job( self.downloadFile, 'interval', seconds=6 )
        # sched.start()

        # gtfsR = GTFSrDB( create=True, delete_all=True )
        self.runEvent()

    def runEvent(self):
        self.downloadFile()
        gtfsR = GTFSrDB( create=True,
                         delete_all=True )  # creo le tabelle se non esistono, tronco le tabelle se sono piene
        self.pbToHdfs()
        self.logToHdfs()

    def downloadFile(self):
        # self.moveFile()
        logger.info( "Download files in pbFile Folder" )
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy( paramiko.AutoAddPolicy() )
        i = 0
        self.listFile = [self.alerts_feed, self.vehicle_feed, self.trip_feed]
        try:
            ssh_client.connect( hostname=self.hostname_data_lake, username=self.username, password=self.password )
            for file in self.listFile:
                ftp_client = ssh_client.open_sftp()
                ftp_client.get( file, self.pb_file + "/" + file.split( "/" )[-1] )
                ftp_client.close()
            ssh_client.close()
            i += 1
        except Exception as e:
            logger.error( "ERRORE downloadFile, hostname: {} file: {} ".format( self.hostname_data_lake, file ), e )

    def moveFile(self):
        logger.info( "Move files in pbFileBackup Folder" )
        if not os.path.isdir( self.pb_file_backup ):
            os.mkdir( self.pb_file_backup )
        for name in os.listdir( self.pb_file ):
            src = os.path.join( self.pb_file, name )
            dest = os.path.join( self.pb_file_backup, name + "_" + datetime.datetime.now().strftime( "%Y%m%d_%H%M%S" ) )
            shutil.move( src, dest )

    def pbToHdfs(self):
        for name in self.listFile:
            local_folder = self.pb_file + "/" + name.split( "/" )[-1]
            pb_name = name.split( "/" )[-1].split( "." )[0] + "_" + datetime.datetime.now().strftime(
                "%Y%m%d_%H%M%S" ) + ".pb"
            self.hdfs_file_path = self.getStringTodayFormat( self.hdfs_file_path )
            self.hdfsUploadAndRemove( self.hdfs_url, self.hdfs_username, self.hdfs_file_path, local_folder, pb_name )

    def logToHdfs(self):
        self.hdfs_log_path = self.getStringTodayFormat( self.hdfs_log_path )
        self.hdfsUploadAndRemove( self.hdfs_url, self.hdfs_username, self.hdfs_log_path, self.logpath, self.logname )


obj = ReadWritePBFile()
