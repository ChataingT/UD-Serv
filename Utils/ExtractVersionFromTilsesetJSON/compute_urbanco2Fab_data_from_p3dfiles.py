# -*- coding: utf-8 -*-
"""
@author: Chataing thibaut
"""

import argparse
import os
import json
import pandas as pd

# Log for debugging purpose
debug = False
def log(msg):
    if debug:
        print(msg)
        
"""
# Init command line argument's parser
# @return : args (Holder of the arguments given in command line)
"""
def init_argument_parser():
        # arg parse
    descr = ''' Desc'''
    
    parser = argparse.ArgumentParser(description=descr)

    in_tileset_path_help = "local path for the input directory. This directory should old a tilset.json and some ./tiles/*.b3dm files "
    parser.add_argument('-in', '--in_path', dest='in_path', type=str, default=os.path.join('.', 'data'), help=in_tileset_path_help)
    
    out_tileset_path_help = "local path for the new tileset.json give as output"
    parser.add_argument('-out', '--out_path', dest='out_path', type=str, default=os.path.join('.', 'data', 'new_tileset.json'), help=out_tileset_path_help)
    
    schema_version_path_help = "local path for the json schema used to define the object Version"
    parser.add_argument('--schema_version_path', dest='schema_version_path', type=str, default=os.path.join('.', 'data', '3DTILES_temporal.version.schema.schema.json'), help=schema_version_path_help)
    
    schema_versionTransition_path_help = "local path for the json schema used to define the object VersionTransation"
    parser.add_argument('--schema_versionTransition_path', dest='schema_versionTransition_path', type=str, default=os.path.join('.', 'data', '3DTILES_temporal.versionTransition.schema.json'), help=schema_versionTransition_path_help)

    schema_scenario_path_help = "local path for the json schema used to define the object Scenario"
    parser.add_argument('--schema_scenario_path', dest='schema_scenario_path', type=str, default=os.path.join('.', 'data', '3DTILES_temporal.scenario.schema.schema.json'), help=schema_scenario_path_help)
    
    schema_workspace_path_help = "local path for the json schema used to define the object Workspace"
    parser.add_argument('--schema_workspace_path', dest='schema_workspace_path', type=str, default=os.path.join('.', 'data', '3DTILES_temporal.workspace.schema.schema.json'), help=schema_workspace_path_help)
    
    debug_help = "Mode debug, adds multiple print info to help know what's happenning"
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help=debug_help)

    args = parser.parse_args()
    
    return args


"""
# Extract inside a .b3dm file the data put in "extensions - 3DTILES_temporal".
# This data is searched between the beginning of the file and the first occurrence of the string "glTF"
# It follows the py3DTiles convention
# @param : file_path (string)
# @return : json_data (dictionnary) {'startDates'=[int], 'endDates'=[int], 'featureIds'=[str]}
"""
def extract_json_from_b3dm(file_path):
    with open(file_path, 'rb') as fd: # open as bytes because b3dm files contain geometric data in bytes 
        stop_reader = str.encode("glTF") # bytes_likes object are needed for the find method of bytes_like
        
        raw_data = fd.read()
        index_end_reading = raw_data.find(stop_reader)
        
        wanted_dirty_data = raw_data[:index_end_reading]
        
        index_start_data = wanted_dirty_data.find(str.encode("{"))
        index_end_data = wanted_dirty_data.rfind(str.encode("}"))
        
        wanted_data = wanted_dirty_data[index_start_data:(index_end_data+1)]

        wanted_data = wanted_data.decode('utf-8')
        
        json_data = json.loads(wanted_data)
        
        json_data = json_data["extensions"]["3DTILES_temporal"]
        log(f"Data found : {json_data}")

    return json_data

"""
# Convert the json data in a dataFrame.
# @param: json_data (dictionnary) {'startDates'=[int], 'endDates'=[int], 'featureIds'=[str]}
# @return: df_data (DataFrame) (columns=['startDate', 'endDate', 'featureId'])
"""
def convert_json_to_DataFrame(json_data):
    startDates = json_data['startDates']
    endDates = json_data['endDates']
    featureIds= json_data['featureIds']
    
    ordered_data = []
    
    for i in range(len(startDates)):
        ordered_data.append([startDates[i], endDates[i], featureIds[i]])
    
    df_data = pd.DataFrame(ordered_data, columns=['startDate', 'endDate', 'featureId'])
    df_data.convert_dtypes()
    return df_data
    
"""
# Get the featureIds startDate and endDate from the .b3dm files found inside the input folder
# @input: folder_path (str) path to a root directory containing .b3dm files
# @return: df_data (DataFrame columns=["featureId", "startDate", "endDate"])
#
# /!\ .b3dm files are supposed to be inside the "tiles" folder (following py3DTiles)
"""    
def get_data_from_b3dms(folder_path):
    if not (os.path.isdir(folder_path)):
        raise IOError(f"Folder path ({folder_path}) is not a directory")
    b3dm_files = []
    for root, directory, files in os.walk(folder_path):
        if root.endswith("tiles"):
            for file in files:
                if file.endswith(".b3dm"):
                    b3dm_files.append(os.path.join(root, file))
            break
    if not b3dm_files : # test if is empty
        raise IOError(f"b3dm files not found in {folder_path}. Search algo stopped at {root}")
    log(f"B3DM files found : {b3dm_files}")
    
    df_data = pd.DataFrame(columns=["featureId", "startDate", "endDate"])
    for path in b3dm_files:
        json_data = extract_json_from_b3dm(path)
        df = convert_json_to_DataFrame(json_data)
        df_data = df_data.append(df)
    df_data = df_data.convert_dtypes()

    log(f"Dataframe describe : \n{df_data.describe(include='all')}\n 5 head rows :\n{df_data.head(5)}")
    return df_data     
    
if __name__ == "__main__":

    args = init_argument_parser()
    
    debug = args.debug
    if debug:
        log("Start with debug (an argument has been passed so debug is ON)")
    else:
        print("Start")
    
    #df_data = extract_json_from_b3dms(args.in_path)
    get_data_from_b3dms(args.in_path)
    print('done')



