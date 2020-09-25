import configparser
import logging

from hdfs import InsecureClient
import os
from datetime import datetime

logger = logging.getLogger( __name__ )
class Utils:
    def getLocalFileName(self, pfFile):
        return self.pb_file + "/" + pfFile.split( "/" )[-1]

    def getConf(class_, config_file, section=None):
        config = configparser.ConfigParser()
        config.read( config_file )
        if section is not None:
            sections = section
        else:
            sections = config.sections()
        for section in sections:
            for item in config.items( section ):
                name = item[0]
                value = item[1]
                # logger.info( name+ ": "+value )
                if value != "":
                    if value.lower() == "true":
                        value = True
                    elif value.lower() == "false":
                        value = False
                setattr( class_, name, value )


    def hdfsUploadAndRemove(self, url, username, hdfs_path, local_path, filename):
        client = InsecureClient( url=url, root="/", user=username )
        # client.makedirs(hdfs_path)
        if os.path.isfile( local_path ):
            hdfs_file = hdfs_path + filename
            client.upload( hdfs_file, local_path )
            os.remove( local_path )

    def getStringTodayFormat(self, stringTime):
        today = datetime.today()
        return stringTime \
            .replace( '%Y', str( today.year ) ) \
            .replace( '%m', "{:02d}".format( today.month ) ) \
            .replace( '%d', "{:02d}".format( today.day ) ) \
            .replace( '%H', "{:02d}".format( today.hour ) ) \
            .replace( '%M', "{:02d}".format( today.minute ) ) \
            .replace( '%S', "{:02d}".format( today.second ) ) \
            .replace( '%f', str( today.microsecond ) )
