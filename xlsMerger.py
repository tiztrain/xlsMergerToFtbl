"""
##########################################################################################

Name: xlsMerger

Purpose: To combine all the excel documents found in a folder, into one geodatabase table

Author: Adam Tolo

Created Date: 1/3/19

App Version: 0.1

ArcGIS Version: 10

Python version: 3.6.5

TODO:
- more logs, especially info

##########################################################################################
"""

# ------------------------------------------------
# importing modules
# ------------------------------------------------

import arcpy, logging, os, sys

# ------------------------------------------------
# setting up logger
# ------------------------------------------------
def init_logger_singleton():
	global logger

	logging.basicConfig(level=logging.DEBUG)
	logger = logging.getLogger(name='mylogger')

	"""create a file handler"""
	handler = logging.FileHandler("//pipelinetrust.com.au/apps/GIS/Projects/CCI_Reporting/_py/runtime.log")
	handler.setLevel(logging.DEBUG)

	"""create a logging format"""
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	handler.setFormatter(formatter)

	"""add the handlers to the logger"""
	logger.addHandler(handler)

        logger.info('LOGGER HAS BEEN CREATED AND CAN BE FOUND AT:\n' + handler)

# ------------------------------------------------
# setting up the environment and variables
# ------------------------------------------------
def environment():    
    arcpy.env.workspace = "//pipelinetrust.com.au/apps/GIS/Projects/CCI_Reporting/
    arcpy.env.overwriteOutput = True
    output_gdb = "data.gdb"
    output_table = "CCI"
    output_temp_table = "temp"
    input_files = '_in/'
    
    	
# ------------------------------------------------
# deleting existing fc and creating a new one
# ------------------------------------------------
def renewFC(input):
	logger.info('CHECKING IF THE FOLLOWING FEATURE CLASS EXISTS:\n[{0}'.format(input))
	if arcpy.Exists(input):
		logger.warning('FILE GEODATABASE FOUND, DELETING & CREATING A NEW ONE...')
                Delete_management (input)
		CreateFileGDB_management (workspace, "data.gdb")
		
	
# ------------------------------------------------
# create a feature table from all spreadsheets in a folder
# ------------------------------------------------
def excelToTable(output_temp, output):
    input_file_list= arcpy.ListFiles('*.xls')
#    Input_Excel_File=input
    Sheet="Operations"

    for xls in input_file_list:
        ExcelToTable_conversion (xls, output_temp, Sheet)
        if not arcpy.Exists(output):
            CreateTable_management (output_gdb, "CCI", output_temp)
        Append_management (output_temp, output)    
    
	
# ------------------------------------------------
# main function
# ------------------------------------------------
def main():
    renewFC(output_gdb)

    excelToTable(output_temp_table, output_table)



if __name__ == '__main__':
    environment()
    init_logger_singleton()
    main()