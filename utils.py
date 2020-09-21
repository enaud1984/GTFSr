import configparser


class Utils:
    def getLocalFileName(self,pfFile):
        return self.pb_file+"/"+pfFile.split("/")[-1]

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
