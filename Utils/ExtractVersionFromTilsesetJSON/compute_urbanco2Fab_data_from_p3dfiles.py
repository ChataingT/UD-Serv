# -*- coding: utf-8 -*-
"""
@author: Chataing thibaut
"""

import argparse
import os
import json
import pandas as pd
import warlock

from compute_versiondata_from_py3dfiles import extract_transactions_from_tilesetJSON, format_data, get_featuresid

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
def extract_json_from_b3dm(debug, file_path):
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
def convert_json_to_DataFrame(debug,json_data):
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
def get_data_from_b3dms(debug,folder_path):
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
        json_data = extract_json_from_b3dm(debug,path)
        df = convert_json_to_DataFrame(debug,json_data)
        df['origin_file_path'] = path
        df_data = df_data.append(df)
    df_data = df_data.convert_dtypes()

    log(f"Dataframe describe : \n{df_data.describe(include='all')}\n 5 head rows :\n{df_data.head(5)}")
    return df_data     

# TODO inclusion ou exclusion des batiments id pour endate==mmillesime
"""
# Extract all featureIds corresponding to one year to put them in one set
# @params: debug (boolean) for log
#           df_features_ids (DataFrame) comming from the extractions of b3dm files
#           df_transactions (DataFrame) comming from the extractions of the tileset.json
#           millesime (int): year 
"""
def get_version_element_for_millesim(debug, df_features_ids, df_transactions, millesime):
    set_featuresIds = get_featuresid(debug,df_transactions, millesime)['version']
    
    subdf = df_features_ids.loc[(df_features_ids['startDate'] <= millesime) & (df_features_ids['endDate'] > millesime)]
    set_featuresIds.union(set(subdf['featureId'].unique()))
    
    return list(set_featuresIds)

def get_version_element_v4(debug, df_features_ids, df_transactions):
    set_featuresIds_tileset = get_featuresid(debug,df_transactions, 2012)['version'] # set from tileset.json
    
    subdf = df_features_ids.loc[(df_features_ids['startDate'] <= 2012) & (df_features_ids['endDate'] > 2012)]
    set_featuresIds_b3dm = set(subdf['featureId'].unique()) #set from b3dms
    
    set_featuresIds_b3dm = set_featuresIds_b3dm.difference(set_featuresIds_tileset) # pop doublet
    
    
def compile_urbanco2Fab_data(debug, 
                             df_ids, 
                             df_transactions, 
                             schema_version_path, 
                             schema_versionTr_path, 
                             schema_scenario_path, 
                             schema_workspace_path):
    log(debug,"-- Compile urbanco2Fab data --")
    
    with open(schema_version_path) as json_file:
        version_json_data = json.load(json_file)
        Version = warlock.model_factory(version_json_data) # Create a class following the model provided by the json schema.

    with open(schema_versionTr_path) as json_file:
        versionTr_json_data = json.load(json_file)
        VersionTr = warlock.model_factory(versionTr_json_data) # Create a class following the model provided by the json schema.
 
    with open(schema_scenario_path) as json_file:
        scenario_json_data = json.load(json_file)
        Scenario = warlock.model_factory(scenario_json_data) # Create a class following the model provided by the json schema.
   
    with open(schema_workspace_path) as json_file:
        workspace_json_data = json.load(json_file)
        Workspace = warlock.model_factory(workspace_json_data) # Create a class following the model provided by the json schema.

    
    set_v1_featuresIds.union(df_ids.loc[''])
    v1 = Version(id="v1",
                 name="2009",
                 description="State in 2009 for the concurrent point of view",
                 startDate="2009",
                 endDate="2009",
                 tags=["concurrent"],
                 featuresIds=list(get_version_element_for_millesim(debug,
                                                                   df_features_ids, 
                                                                   df_transactions, 
                                                                   2009))
                 )
    v2 = Version(id="v2",
                 name="2012",
                 description="State in 2012 for the concurrent point of view",
                 startDate="2012",
                 endDate="2012",
                 tags=["concurrent"],
                 featuresIds=list(get_version_element_for_millesim(debug,
                                                                   df_features_ids, 
                                                                   df_transactions, 
                                                                   2012))
                 )
    v3 = Version(id="v3",
                 name="2015",
                 description="State in 2015 for the concurrent point of view",
                 startDate="2015",
                 endDate="2015",
                 tags=["concurrent"],
                 featuresIds=list(get_version_element_for_millesim(debug,
                                                                   df_features_ids, 
                                                                   df_transactions, 
                                                                   2015))
                 )
    
    v4 = Version(id="v4",
                 name="2011",
                 description="State in 2015 for the concurrent point of view",
                 startDate="2011",
                 endDate="2011",
                 tags=["proposition"],
                 featuresIds=list(get_version_element_for_millesim(debug,
                                                                   df_features_ids, 
                                                                   df_transactions, 
                                                                   2012))
                 )
    v5 = Version(id="v5",
                 name="2014",
                 description="State in 2009 for the concurrent point of view",
                 startDate="2014",
                 endDate="2014",
                 tags=["proposition"],
                 featuresIds=list(get_version_element_for_millesim(debug,
                                                                   df_features_ids, 
                                                                   df_transactions, 
                                                                   2012))
                 )
    
if __name__ == "__main__":

    args = init_argument_parser()
    
    debug = args.debug
    if debug:
        log("Start with debug (an argument has been passed so debug is ON)")
    else:
        print("Start")
    
    # Get all featureIds with their startDate and endDate from the b3dm files
    df_features_ids = get_data_from_b3dms(debug,args.in_path)
    
    # Get all transactions from the tileset.json
    tilset_path = os.path.join(args.in_path, "tileset.json")
    list_transactions = extract_transactions_from_tilesetJSON(debug, tilset_path)
    df_transactions = format_data(debug, list_transactions)
    
    # j'ai tout les transactions et les features ids
    
    """
    v1 = 
    
    """
    df_transaction_2009_2012_all = df_transactions.loc[df_transactions['startDate'] == 2009]
    df_transaction_2009_2012_all.reset_index(drop=True, inplace=True)
    id = round(len(df_transaction_2009_2012_all)*0.4)
    df_transaction_2009_2011 = df_transaction_2009_2012_all.iloc[:id]
    df_transaction_2009_2011.reset_index(drop=True, inplace=True)

    df_transaction_2009_2012 = df_transaction_2009_2012_all.iloc[id:]
    df_transaction_2009_2012.reset_index(drop=True, inplace=True)

    
    id = 0
    limit = round(len(df_transaction_2009_2012_all)*0.4)
    df_transaction_2009_2011 = pd.DataFrame(columns=df_transaction_2009_2012_all.columns)
    df_transaction_2009_2012 = pd.DataFrame(columns=df_transaction_2009_2012_all.columns)
    df_features_ids_2011 = pd.DataFrame(columns=df_features_ids.columns)
    df_features_ids_2012 = pd.DataFrame(columns=df_features_ids.columns)

    for row_index,row in df_transaction_2009_2012_all.iterrows():
        list_featureId = row["source"] + row["destination"]
        if id <= limit:
            df_transaction_2009_2011 = df_transaction_2009_2011.append(row)
            for fi in list_featureId:   
                df_features_ids_2011 = df_features_ids_2011.append(df_features_ids.loc[(df_features_ids["featureId"] == fi)])
        else:
            df_transaction_2009_2012 = df_transaction_2009_2012.append(row)
            for fi in list_featureId:   
                df_features_ids_2012 = df_features_ids_2012.append(df_features_ids.loc[(df_features_ids["featureId"] == fi)])
        id += 1
        df_features_ids_2011 = df_features_ids_2011.loc[df_features_ids_2011["endDate"] != 2009]
        df_features_ids_2012 = df_features_ids_2012.loc[df_features_ids_2012["endDate"] != 2009]
        
    #good 2009 - 2011 - 2012
    
    df_transaction_2012_2015_all = df_transactions.loc[df_transactions['startDate'] == 2012]
    
    df_transaction_2011_2013 = pd.DataFrame(columns=df_transaction_2009_2012_all.columns)
    df_transaction_2012_2015 = pd.DataFrame(columns=df_transaction_2009_2012_all.columns)
    
    for row_index1,row1 in df_features_ids_2011.iterrows():
        featureId = row1["featureId"]
        for row_index,row in df_transaction_2012_2015_all.iterrows():
            list_featureId = set(row["source"] + row["destination"])
            if featureId in list_featureId:
                df_transaction_2011_2013 = df_transaction_2011_2013.append(row)
    
    df_transaction_2012_2015 = df_transaction_2012_2015_all.copy()

    df_transaction_2012_2015['source'] = df_transaction_2012_2015['source'].apply(tuple)
    df_transaction_2012_2015['destination'] = df_transaction_2012_2015['destination'].apply(tuple)
    df_transaction_2012_2015['type'] = df_transaction_2012_2015['type'].apply(str)
    df_transaction_2012_2015['transactions'] = df_transaction_2012_2015['transactions'].apply(str)
    
    df_transaction_2011_2013['source'] = df_transaction_2011_2013['source'].apply(tuple)
    df_transaction_2011_2013['destination'] = df_transaction_2011_2013['destination'].apply(tuple)
    df_transaction_2011_2013['type'] = df_transaction_2011_2013['type'].apply(str)
    df_transaction_2011_2013['transactions'] = df_transaction_2011_2013['transactions'].apply(str)
    df_transaction_2011_2013.drop_duplicates(inplace=True)
    
    df_transaction_2012_2015 = df_transaction_2012_2015.merge(df_transaction_2011_2013, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']    

    # good transition 2011-2013 et 2012-2015
    
    df_features_ids_2013 = pd.DataFrame(columns=df_features_ids.columns)
    fi = df_transaction_2011_2013['destination'].apply(list).tolist()
    fi = [item for sublist in fi for item in sublist]
    for row_index1,row in df_features_ids.iterrows():
        if row['featureId'] in fi:
            df_features_ids_2013 = df_features_ids_2013.append(row)    
    print('done')



