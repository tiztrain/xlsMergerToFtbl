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
print("importing modules...")

import arcpy, logging, os, sys, datetime

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
    logger.info('Setting up environment...\n')
##    arcpy.env.workspace = "//pipelinetrust.com.au/apps/GIS/Projects/CCI_Reporting/"
    arcpy.env.overwriteOutput = True
    workspace = "//pipelinetrust.com.au/apps/GIS/Projects/CCI_Reporting/"
    return workspace

# ------------------------------------------------
# deleting existing fc and creating a new one
# ------------------------------------------------
def renewFC(workspace, input):
    logger.info('CHECKING IF THE FOLLOWING FEATURE CLASS EXISTS:\n%s', input + '\n')
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
def excelToTable(workspace, output_gdb, output_temp, output, input_folder):
    logger.info('CONVERSION FROM XLSX TO FILE GEODATABASE TABLE...\n' + '\n')
    input_file_list = [f for f in os.listdir(input_folder) if os.path.isfile(os.path.join(input_folder, f))]
    sheet="Operations"
    count_xlsx = 0
    total_xlsx = len(input_file_list)
    acceptedFieldList = returnAcceptedFieldList()

    logger.info('There are {0} excel spreadsheets to convert\n'.format(total_xlsx))

    for xlsx in input_file_list:
        logger.info('At {0} of {1} conversions'.format(count_xlsx, total_xlsx))
        logger.info('running conversion of: %s ', input_file_list[count_xlsx])
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

        logger.info('deleting original fields that cause issue with schema...')
        fieldsToDelete(acceptedFieldList, output_temp)
        
        if not arcpy.Exists(output):
            logger.info('creating CCI table...')
            arcpy.CreateTable_management (output_gdb, 'CCI', output_temp)

        # if the append fails, show what the differences are in the schema
        try:    
            logger.info('appending to output table... \n')
            arcpy.Append_management (output_temp, output)
            count_xlsx += 1
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
            2: ['Comments_Actions_Req', 'TEXT', '', '', '1000', 'Comments Actions Req']
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
    acceptedFieldList = ['OBJECTID', 'Pipeline_Patrol_State', 'Sighting_Status', 'ID', 'Pipeline_Patrol_Program'
                         'Submitted_By', 'Observation_Date', 'Sighting_Classification',
                         'Comments_Actions_Req', 'Location', 'KP', 'Latitude', 'Longitude',
                         'Resolved', 'Resolved_Date', 'Resolved_By']
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
            logging.info('the s% field has been dropped', field.name)


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
    logger.info('outputed text file of the schema differences to the following location: s%', compare_file)


# ------------------------------------------------
# validates the fields to make sure they are the correct type, if not converts to correct field type
# ------------------------------------------------
def fieldTypeConverter(output_temp):
    fields = arcpy.ListFields(output_temp)
    for field in fields:
        field_name = field.name
        field_type = field.type
        ### this if statement is used to skip the ConvertTimeField tool if the field is already a date type
        if field_name == 'Resolved_Date' or field_name == 'Observation_Date':
            if not field_type == 'Date':
                logger.info('')
                logger.info('field name is : {0} and field type is: {1}'.format(field_name, field_type))
                logger.info('transferring values to new fields...')
                ### using this tool to transfer the vales from a string to a standard date format
                logger.info('converting the Date string field to a temp Date field...')
                arcpy.ConvertTimeField_management (in_table=output_temp,
                                                   input_time_field=field_name,
                                                   input_time_format='dd/MM/yyyy',
                                                   output_time_field='temp',
                                                   output_time_type='DATE',
                                                   output_time_format='dd/MM/yyyy')
                ### delete the original resolved date field which has the incorrect schema
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field=field_name)
                logger.info('adding new s% field with updated schema...', field_name)
                ### re-add updated resolved date field
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name=field_name,
                                                  field_type='Date',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='',
                                                  field_alias='')
                logger.info('temp field values to s% field...', field_name)
                ### copy values to updated resolved field
                arcpy.CalculateField_management(in_table=output_temp,
                                            field=field_name,
                                            expression='!temp!',
                                            expression_type='PYTHON_9.3',
                                            code_block=None)
                arcpy.DeleteField_management (in_table=output_temp,
                                          drop_field='temp')
        ### the following is used to make the numeric fields all doubles
        if field_name == 'KP' or field_name == 'Latitude' or field_name == 'Longitude':
            if not field_type == 'Double':
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
                                                  field_alias='')
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
        if (field_name == 'Pipeline_Patrol_State' or field_name == 'Sighting_Status'
        or field_name == 'Pipeline_Patrol_Program' or field_name == 'Submitted_By'
        or field_name == 'Sighting_Classification' or field_name == 'Location'
        or field_name == 'Resolved_By' or field_name == 'Corridor_Inspection_Classification'):
            ### converting values in corridor inspection classification to sighting classification
            if field_name == 'Corridor_Inspection_Classification':
                arcpy.AddField_management(in_table=output_temp,
                                                  field_name='Sighting_Classification',
                                                  field_type='String',
                                                  field_precision='',
                                                  field_scale='',
                                                  field_length='255',
                                                  field_alias='')
            if not field_type == 'String':
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
                                                  field_alias='')
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
# main function
# ------------------------------------------------
def main():
    init_logger_singleton()

    # define global variables
    workspace = environment()
    output_gdb = workspace + "data.gdb/"
    output_table = output_gdb + "CCI"
    output_temp_table = output_gdb + "temp"
    input_folder = workspace + '_in/'

    renewFC(workspace, output_gdb)
    excelToTable(workspace, output_gdb, output_temp_table, output_table, input_folder)
    
    logger.info('SCRIPT FINISHED')


if __name__ == '__main__':    
    main()
