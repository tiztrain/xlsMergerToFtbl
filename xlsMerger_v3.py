"""
##########################################################################################

Name: xlsMerger

Purpose: To combine multiple excel documents, into one geodatabase table

Author: Adam Tolo

Created Date: 1/3/19

App Version: 0.3

ArcGIS Version: 10.3

Python version: 2.7

PREP STEPS:
Before running the script, copy all the spreadsheets found at:
http://thehub.apa.com.au/workareap/ID/IPP/Corridor%20Condition%20Reports/Field%20Services to
\\pipelinetrust.com.au\apps\GIS\Projects\CCI_Reporting\_in

TODO:
- if Resolved field == null change to No (check for other values)
- lock down spreadsheet so changes to schema aren't made. create domain set for at least Resolved field
- add try:except for the whole script to record error if it occurs
- make script grab files from the HUB and copy them to the workspace in folder
- put all adjustable variables in a config function
- clean up logger files are not in modules/functions
- make functions/models more reuseable
- can i use the return logger function instead of the global logger variable

##########################################################################################
"""

# ------------------------------------------------
# importing modules
# ------------------------------------------------
print("importing modules...")

import arcpy, logging, os, sys, datetime, re, shutil
from itertools import islice

# ------------------------------------------------
# setting up logger
# ------------------------------------------------
def init_logger_singleton():
    print("setting up logger...")
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

    logger.info('LOGGER HAS BEEN CREATED AND CAN BE FOUND AT: \n %s \n', str(handler))


# ------------------------------------------------
# setting up the environment and variables
# ------------------------------------------------
def environment():
##    arcpy.env.workspace = "//pipelinetrust.com.au/apps/GIS/Projects/CCI_Reporting/"
    arcpy.env.overwriteOutput = True
    workspace = "//pipelinetrust.com.au/apps/GIS/Projects/CCI_Reporting/"
    return workspace


# ------------------------------------------------
# copy input spreadsheets from source to destination
# ------------------------------------------------
def copyFiles(src, dest):
    src_files = os.listdir(src)
    for file_name in src_files:
        full_file_name = os.path.join(src, file_name)
        if (os.path.isfile(full_file_name)):
            shutil.copy(full_file_name, dest)
            

# ------------------------------------------------
# deleting existing fc and creating a new one
# ------------------------------------------------
def renewFC(workspace, input):
    if arcpy.Exists(input):
        logger.info('File Geodatabase found. Deleting... ' + '\n')
        arcpy.Delete_management (input)
        logger.info('Creating a new geodatabase...' + '\n')
        arcpy.CreateFileGDB_management (workspace, "data.gdb")
        logger.info('gdb created...' + '\n')
    else:
        logger.info('File geodatabase not found, creating a new one...' + '\n')
        arcpy.CreateFileGDB_management (workspace, "data.gdb")
        logger.info('.gdb created...' + '\n')


# ------------------------------------------------
# create a feature table from all spreadsheets in a folder
# ------------------------------------------------
def excelToTable(workspace, output_gdb, output_temp, output, input_folder, start_xlsx):
    # creates a list of all the xlsx file found in the folder
    input_file_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f)) and f.endswith('.xlsx')]
    sheet="Operations"
    # lists the current spreadsheet to be converted (eg. it is at number 15 of 118 spreadsheets to complete) 
    count_xlsx = 1
    total_xlsx = len(input_file_list)
    acceptedFieldList = returnAcceptedFieldList()

    logger.info('There are {0} excel spreadsheets to convert\n'.format(total_xlsx))

    # start script at file number that caused issue (input, start, end)
    for xlsx in islice(input_file_list, start_xlsx, None):
        logger.info('Converting number {0} of {1} spreadsheets to convert'.format(count_xlsx, total_xlsx))
        logger.info('Spreadsheet name: %s ', input_file_list[start_xlsx])
        arcpy.ExcelToTable_conversion (input_folder + xlsx, output_temp, sheet)
        logger.info('completed conversion...')

        ### adjust temp table's schema
        logger.info('updating temp table schema...')
        logger.info('adding new fields...')
        addFields(output_temp)

        ### list fields and their information
        fieldInfo(output_temp)
        
        ### update field types in preparation for the append
        fieldTypeConverter(output_temp)

        # if any fields from the spreadsheet template are missing, add it to the temp table
        # must be done after the excel to table conversion, otherwise fields will be added with no values
        # and when the conversion trys to take place it sees the field and does not copy the values over
        logger.info('checking for missing fields from the spreadsheet template that need to be added...')
        fieldsToAdd(acceptedFieldList, output_temp)

        logger.info('deleting original fields that cause issue with schema...')
        fieldsToDelete(acceptedFieldList, output_temp)

        # if CCI table does not exist, create it based off the temp folder
        if not arcpy.Exists(output):
            logger.info('creating CCI table...')
            arcpy.CreateTable_management (output_gdb, 'CCI', output_temp)

        # if the append fails, show what the differences are in the schema
        try:    
            logger.info('appending to output table... \n')
            arcpy.Append_management (output_temp, output)
            count_xlsx += 1
            start_xlsx += 1
        except:
            logger.info('APPEND FAILED due to schema differences\nSCRIPT STOPPED')
            compareTables(output, output_temp, workspace)
            exit()

        logger.info('deleting temp table... \n')
        arcpy.Delete_management (in_data=output_temp)
        logger.info('moving to next spreadsheet... \n')


# -----------------------------------------
# add fields and their properties to the designated fc using a dictionary and lists
# -----------------------------------------
def addFields(in_table):
    logger.info("adding fields to " + in_table + '\n')
    ### defining field, type, precision, scale, length and alias
    DIC_field_names_type = {
            2: ['Comments_Actions_Req', 'TEXT', '', '', '2000', 'Comments Actions Req']
    }

    """add fields to the feature class based off the values in dictionary"""
    for key, value in DIC_field_names_type.iteritems():
                    arcpy.AddField_management(in_table=in_table,
                                              field_name=value[0],
                                              field_type=value[1],
                                              field_precision=value[2],
                                              field_scale=value[3],
                                              field_length=value[4],
                                              field_alias=value[5])


# ------------------------------------------------
# display information about fields; names, types and lengths
# ------------------------------------------------
def fieldInfo(in_table):
    logger.info("displaying field names and type:")
    fields = arcpy.ListFields(in_table)

    for field in fields:
        logger.info("%s is a type of %s with a length of %s"
              ,field.name, field.type, field.length)


# ------------------------------------------------
# used as the list of accepted fields for the table
# ------------------------------------------------
def returnAcceptedFieldList():
    # Latitude and longitude are deleted cause they will need to be re-created once in a table
    acceptedFieldList = ['OBJECTID', 'Pipeline_Patrol_State', 'Sighting_Status', 'ID', 'Pipeline_Patrol_Program',
                         'Submitted_By', 'Observation_Date', 'Sighting_Classification',
                         'Comments_Actions_Req', 'Location', 'KP', 'Resolved', 'Resolved_Date', 'Resolved_By',
                         'MP_for_RBP_only', 'APA_Encroachment_Number', 'Corridor_Inspection_Classification']
    return acceptedFieldList


# ------------------------------------------------
# deletes fields that are not defined in the list of accepted fields
# ------------------------------------------------
def fieldsToDelete(acceptedFields, in_table):
    fields = arcpy.ListFields(in_table)

    for field in fields:
        if field.name not in acceptedFields:
            arcpy.DeleteField_management (in_table=in_table,
                                      drop_field=field.name)
            logging.info('the %s field has been dropped', field.name)


# ------------------------------------------------
# adds fields that are missing from the excel template spreadsheet
# ------------------------------------------------
def fieldsToAdd(acceptedFields, in_table):
    fieldList = arcpy.ListFields(in_table)
    fieldNames = []
    for field in fieldList:
        fieldNames.append(field.name)
    
    for acceptedField in acceptedFields:
        if acceptedField not in fieldNames:
            # creating field as String. If type needs to be adjusted, this can be setup in the fieldTypeConverter
            arcpy.AddField_management(in_table=in_table,
                                              field_name=acceptedField,
                                              field_type='TEXT',
                                              field_precision='',
                                              field_scale='',
                                              field_length='255',
                                              field_alias='')           


# ------------------------------------------------
# compare the schemas of the input and output table
# ------------------------------------------------
def compareTables(base_table, test_table, workspace):
    # Set local variables
    sort_field = "OBJECTID"
    compare_type = "SCHEMA_ONLY"
    ignore_option = ""
    attribute_tolerance = ""
    omit_field = ""
    continue_compare = "CONTINUE_COMPARE"
    compare_file = workspace + "_py/schema_issues.txt"

    # Process: FeatureCompare
    compare_result = arcpy.TableCompare_management(
        base_table, test_table, sort_field, compare_type, ignore_option, 
        attribute_tolerance, omit_field, continue_compare, compare_file)
    logger.info('outputed text file of the schema differences to the following location: %s', compare_file)


# ------------------------------------------------
# validates the fields to make sure they are the correct type, if not converts to correct field type
# ------------------------------------------------
def fieldTypeConverter(output_temp):
    fields = arcpy.ListFields(output_temp)
    for field in fields:
        field_name = field.name
        field_type = field.type
        # original field alias
        field_alias_org = field.aliasName
        # updated alias needed for of schema issues with the spreadsheet
        field_name_alias_updated = field_name.replace('_', ' ')
        ### this if statement is used to skip the ConvertTimeField tool if the field is already a date type
        if field_name == 'Resolved_Date' or field_name == 'Observation_Date':
            # also if the field alias is incorrect, the following scripts will fix the field up
            if not field_type == 'Date' or field_alias_org != field_name_alias_updated:
                logger.info('')
                logger.info('field name is : {0} and field type is: {1}'.format(field_name, field_type))
                logger.info('transferring values to new fields...')
                ### add temp date field
                logger.info('adding temp date field...')
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name='temp',
                                                  field_type='Date',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='',
                                                  field_alias=field_name_alias_updated)
                ### convert strings that are in a date/month/year format into a date field
                ### replace blank values or invalid strings with null. Need to do this because some blank values come across
                ### as 01/01/2001. 
                logger.info('replacing blank values and strings with nulls as it causes an error with conversions...')
                codeblock = ("import datetime" + "\n" +
                            "def fixDate(date):" + "\n" +
                            "  if date == '':" + "\n" +
                            "    return None" + "\n" +
                            "  elif isinstance(date, basestring):" + "\n" +
                            "    try:" + "\n" +
                            "        output = datetime.datetime.strptime(date, '%d/%m/%Y')" + "\n" +
                            "        return output" + "\n" +
                            "    except:" + "\n" +
                            "        return None" + "\n" +                             
                            "  else:" + "\n" +
                            "    return None")
                arcpy.CalculateField_management(in_table=output_temp,
                                            field='temp',
                                            expression='fixDate(!' + field_name + '!)',
                                            expression_type='PYTHON_9.3',
                                            code_block=codeblock)
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field=field_name)
                logger.info('adding new %s field with updated schema...', field_name)
                ### re-add updated resolved date field
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name=field_name,
                                                  field_type='Date',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='',
                                                  field_alias=field_name_alias_updated)
                logger.info('temp field values to %s field...', field_name)
                ### copy values to updated resolved field
                arcpy.CalculateField_management(in_table=output_temp,
                                            field=field_name,
                                            expression='!temp!',
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field='temp')
                
        # making sure ID is the field type long        
        elif field_name == 'ID':
            # also if the field alias is incorrect, the following scripts will fix the field up
            if not field_type == 'Long' or field_alias_org != field_name_alias_updated:
                logger.info('')
                logger.info('field name is : {0} and field type is: {1}'.format(field_name, field_type))
                logger.info('transferring values to new fields...')
                ### using this tool to transfer the vales to a long format
                logger.info('creating temp field with the long format...')
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name='temp',
                                                  field_type='Long',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='',
                                                  field_alias='')
                ### replace blank values or strings with null
                logger.info('replacing blank values and strings with nulls as it causes an error with conversions...')
                codeblock = """def fixID(id):
                        if isinstance(id, basestring):
                            return None
                        elif id == '':
                            return None
                        else:
                            return int(id)
                        """
                ### copy cleaned values to a temp field
                arcpy.CalculateField_management(in_table=output_temp,
                                            field='temp',
                                            expression='fixID(!ID!)',
                                            expression_type='PYTHON_9.3',
                                            code_block=codeblock)
                logger.info('deleting original field...')
                ### delete the original field which has the incorrect schema
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field=field_name)
                logger.info('add original field with updated schema...')
                ### re-add schema updated field a copy values over
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name=field_name,
                                                  field_type='Long',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='',
                                                  field_alias=field_name_alias_updated)
                logger.info('copying values to updated original field...')
                ### copy values to updated resolved field
                arcpy.CalculateField_management(in_table=output_temp,
                                            field=field_name,
                                            expression='!temp!',
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field='temp')
                
        ### the following is used to make the numeric fields all doubles
        elif field_name == 'KP':
            # also if the field alias is incorrect, the following scripts will fix the field up
            if not field_type == 'Double' or field_alias_org != field_name_alias_updated:
                ### using this tool to transfer the vales from a string to a standard date format
                logger.info('')
                logger.info('field name is : {0} and field type is: {1}'.format(field_name, field_type))
                logger.info('creating a temp Double field...')
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name='temp',
                                                  field_type='Double',
                                                  field_precision='9',
                                                  field_scale='6',
                                                  field_length='',
                                                  field_alias='')
                ### copy values to temp field
                logger.info('copying values to temp field...')
                arcpy.CalculateField_management(in_table=output_temp,
                                            field='temp',
                                            expression='!' + field_name + '!',
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
                logger.info('deleting original field...')
                ### delete the original field which has the incorrect schema
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field=field_name)
                logger.info('add original field with updated schema...')
                ### re-add schema updated field a copy values over
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name=field_name,
                                                  field_type='Double',
                                                  field_precision='9',
                                                  field_scale='6',
                                                  field_length='',
                                                  field_alias=field_name_alias_updated)
                logger.info('copying values to updated original field...')
                ### copy values to updated resolved field
                arcpy.CalculateField_management(in_table=output_temp,
                                            field=field_name,
                                            expression='!temp!',
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field='temp')

        ### the following is used to make check if a field is the correct type
        elif (field_name == 'Pipeline_Patrol_State' or field_name == 'Sighting_Status'
        or field_name == 'Pipeline_Patrol_Program' or field_name == 'Submitted_By'
        or field_name == 'Sighting_Classification' or field_name == 'Location'
        or field_name == 'Resolved_By' or field_name == 'Corridor_Inspection_Classification'):
            # also if the field alias is incorrect, the following scripts will fix the field up
            if not field_type == 'String' or field_alias_org != field_name_alias_updated:
                ### using this tool to transfer the vales from one type to another
                logger.info('')
                logger.info('field name is : {0} and field type is: {1}'.format(field_name, field_type))
                logger.info('converting the field to another type...')
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name='temp',
                                                  field_type='String',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='255',
                                                  field_alias='')
                ### copy values to temp field
                logger.info('copying values to temp field...')
                arcpy.CalculateField_management(in_table=output_temp,
                                            field='temp',
                                            expression='!' + field_name + '!',
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
                logger.info('deleting original field...')
                ### delete the original field which has the incorrect schema
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field=field_name)
                logger.info('add original field with updated schema...')
                ### re-add schema updated field a copy values over
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name=field_name,
                                                  field_type='String',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='255',
                                                  field_alias=field_name_alias_updated)
                logger.info('copying values to updated original field...')
                ### copy values to updated field
                arcpy.CalculateField_management(in_table=output_temp,
                                            field=field_name,
                                            expression='!temp!',
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
                # delete temp field
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field='temp')

            
    arcpy.CalculateField_management(in_table=output_temp,
                                    field="Comments_Actions_Req",
                                    expression='!Comments___Actions_Req!',
                                    expression_type='PYTHON_9.3',
                                    code_block=None)

# ------------------------------------------------
# create lat and long fields
# ------------------------------------------------
def createLatLong(output):
    # create lat and long fields
    arcpy.AddField_management(in_table=output,
                                      field_name='Latitude',
                                      field_type='Double',
                                      field_precision='9',
                                      field_scale='6',
                                      field_length='',
                                      field_alias='')
    arcpy.AddField_management(in_table=output,
                                      field_name='Longitude',
                                      field_type='Double',
                                      field_precision='9',
                                      field_scale='6',
                                      field_length='',
                                      field_alias='')


# ------------------------------------------------
# this function will populate a Latitude and Longitude field in a table
# ------------------------------------------------
def calcLatLong(output):
    # define variables
    fields = arcpy.ListFields(output)
    # cleanup the location field values so that special characters don't break the script
    # and points are placed in the middle of Australia
    logger.info('spliting location field into latitude and longitude fields...')
    with arcpy.da.UpdateCursor(in_table=output,
                                   field_names=['Location','Latitude','Longitude']) as cursor:
        for row in cursor:
            if row[0] is None:
                logger.info('%s has been changed due to incorrect format: null', row[0])
                row[0] = '-26.006099,133.952746'
                cursor.updateRow(row)
            if re.search(r'[a-zA-Z]', row[0]):
                logger.info('%s has been changed due to incorrect format: letters found', row[0])
                row[0] = '-26.006099,133.952746'
                cursor.updateRow(row)
            if re.search(r'^(.*,.*,.*)$', row[0]):
                logger.info('%s has been changed due to incorrect format: more' +
                            ' than 1 comma found', row[0])
                row[0] = '-26.006099,133.952746'
                cursor.updateRow(row)
            if re.search(r'^[^,]+$', row[0]):
                logger.info('%s has been changed due to incorrect format: no comma found', row[0])
                row[0] = '-26.006099,133.952746'
                cursor.updateRow(row)   
            if re.search(r'.*[.].*[.].*[.].*', row[0]):
                logger.info('%s has been changed due to location containing three full stops', row[0])
                row[0] = '-26.006099,133.952746'
                cursor.updateRow(row)
            if row[0] == '':
                logger.info('%s has been changed due to empty location value', row[0])
                row[0] = '-26.006099,133.952746'
                cursor.updateRow(row)
            else:
##                print ('{0} has passed'.format(row[0]))
                pass
            latitude, longitude = row[0].split(',',1)
            row[1] = float(latitude) 
            row[2] = float(longitude)
            cursor.updateRow(row)
    logger.info('latitude and longitude fields are populated...')
    del cursor


# ------------------------------------------------
# this function will use the Latitude and Longitude fields in the table to c
# ------------------------------------------------
def createXYEvent(in_table, output):
    x_field = 'Longitude'
    y_field = 'Latitude'
    z_field = ''
    # 4283 is the code for: 
    #  GCS_GDA_1994 (Meters)
    coordinate_system = arcpy.SpatialReference(4283)
    arcpy.XYTableToPoint_management (in_table, output, x_field, y_field, z_field, coordinate_system)
    

# ------------------------------------------------
# main function
# ------------------------------------------------
def main():
    # setup logger
    init_logger_singleton()

    ### define global variables
    # ------------------------------------------------
    logger.info('Setting up environment & variables...\n')
    workspace = environment()
    output_gdb = workspace + "data.gdb/"
    output_table = output_gdb + "CCI"
    output_fc = output_gdb + 'Corridor_Condition_Reports'
    output_temp_table = output_gdb + "temp"
    input_folder = workspace + '_in/'
    src_xlsx = r'http://thehub.apa.com.au/workareap/ID/IPP/Corridor%20Condition%20Reports/Field%20Services'
    # defines the starting location of the list for the islice for loop 
    xlsx_start_pt = 0
    # ------------------------------------------------


##    logger.info('copying files from source location and moving to destination')
##    copyFiles(src_xlsx, input_folder)

    logger.info('CHECKING IF THE FOLLOWING FEATURE CLASS EXISTS:\n%s', output_gdb + '\n')
    renewFC(workspace, output_gdb)

    logger.info('CONVERSION FROM XLSX TO FILE GEODATABASE TABLE...\n\n')    
    excelToTable(workspace, output_gdb, output_temp_table, output_table, input_folder, xlsx_start_pt)

    logger.info('creating Latitude and Longitude fields...')
    createLatLong(output_table)

    logger.info('checking if location value has data errors.  co-ord -26.006099,133.952746 is assigned to any errors...')
    calcLatLong(output_table)

    logger.info('create feature class from CCI table...')
    createXYEvent(output_table, output_fc)
    
    logger.info('SCRIPT FINISHED')


if __name__ == '__main__':    
    main()
