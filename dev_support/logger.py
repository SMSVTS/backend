"""
====================================================================================
File                :   logger.py
Description         :   Contains logging script support
Author              :   Murugesan G
Date Created        :   Feb 7th 2023
Last Modified BY    :   Murugesan G
Date Modified       :   Feb 9th 2023
====================================================================================
"""
import logging
import logging.handlers as handlers
import time
# import collections
# import itertools
# import linecache
# import sys
import traceback

TimeString = time.strftime("%Y%m%d")
log_filename = TimeString + '_jarus_api'

###############################################################################
# LOGGING CONFIGURATION
log_dir_path = "logs"

# logger name will be given of the module name
logger = logging.getLogger(__name__)

"""
Help Regarding  Logging messages which are less severe than level will be 
ignored

Below is the order for the logging level

---------------------------
Level	    Numeric value
---------------------------
CRITICAL	50
ERROR	    40
WARNING	    30
INFO	    20
DEBUG	    10
NOTSET	    0
---------------------------
"""
# Here we define our Logging Level
logger.setLevel(logging.INFO)

# Here we define our formatter
formatter = logging.Formatter(
    '%(asctime)s.%(msecs)03d %(levelname)s %(filename)s %(module)s - %('
    'funcName)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# log file name and location declaration

file_name = '{}/{}.log'.format(log_dir_path, log_filename)


"""
Help Regarding setting the time rotating interval

------------------------------------------------------------------------------
Value	    Type of interval	If/how atTime is used
------------------------------------------------------------------------------
'S'	        Seconds	            Ignored
'M'	        Minutes	            Ignored
'H'	        Hours	            Ignored
'D'	        Days	            Ignored
'W0'-'W6'	Weekday (0=Monday)	Used to compute initial rollover time
'midnight'	Roll over at midnight, if atTime not specified, else at time atTime
Used to compute initial rollover time
-------------------------------------------------------------------------------
"""

logHandler = handlers.TimedRotatingFileHandler(file_name, when='midnight',
                                               interval=1, backupCount=30,
                                               encoding=None, delay=False,
                                               utc=False, atTime=None)

logHandler.setFormatter(formatter)

logger.addHandler(logHandler)


def log_info(**info):
    """
    This function will handle to log the info
    :param info: object
    :return: None
    """
    logger.info(info)


def log_error(e):
    """
    This function will handle to log the error
    :return: None
    """
    print(traceback.print_exc())
    logger.error(traceback.format_exc())


def log_warning(warning):
    """
    This function will handle to log the warning
    :param warning: object
    :return: None
    """
    print(traceback.format_exc())
    logging.warning(traceback.format_exc())


def log_debug(debug):
    """
    This function will handle to log the debug
    :param debug: object
    :return: None
    """
    print(traceback.format_exc())
    logging.debug(traceback.format_exc())