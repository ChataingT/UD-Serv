# -*- coding: utf-8 -*-
"""

@Author : Thibaut Chataing   
@Date : 12/05/2020
# Script for UD-CPOV POC

This script should :
1. Load tileset.json
2. Extract information to compile versions and versionTransitions
3. Create a new tileset.json with the versions and versionTransitions data

/!\ Requirement :
    pandas (use of dataframe)
    warlock (use to load json schema as template)
"""

import warlock
import argparse
import json
import os
import pandas as pd


debug = False # By default no log wanted

# Log for debugging purpose
def log(debug,msg):
    if debug:
        print(msg)


"""
# Load the file .\data\tileset.json as a json
# In the attributs : extension-3DTILES_temporal it extracts all transactions
# @param: tileset_path (str)
# @return : list_transactions (list[dict])
"""
def extract_transactions_from_tilesetJSON(debug,tileset_path):
    log(debug,"-- Load tileset.json --")
    list_transactions = []
    with open(tileset_path) as json_file:
        data = json.load(json_file)
        startDate = data['extensions']['3DTILES_temporal']['startDate']
        endDate = data['extensions']['3DTILES_temporal']['endDate']
        list_transactions = data['extensions']['3DTILES_temporal']['transactions']

    log(debug,f"startDate = {startDate} \nendDate = {endDate}\nlist_transactions = {list_transactions}")
    return list_transactions

"""
# Push the data inside a DataFrame and add type to it
# @param : list_tr (list[dict])
# @return : transactions (DataFrame)
"""
def format_data(debug,list_tr):
    log(debug,"-- Format data --")
    
    # Instance a DataFrame with the data found inside the named columns
    transactions = pd.DataFrame(list_tr, columns=["id", "startDate", "endDate", "source", "destination", "type", "transactions"])

    #Auto detect of type
    transactions = transactions.convert_dtypes()

    log(debug,f"Describe data : {transactions.describe(include=['string', 'Int64'])}")
    
    return transactions


"""
# Extracte from a DataFrame all the IDs of the batiment that should be present in the version.
# The extraction is based on the transactions (transformation of a building between two times)
# A version represents a year (ex: 2009, 2012, 2015)
# When a transactions start at or before the year and end after we take the source id
# When a transactions end at the year we take the destination id
#
# @Param:
#   transaction : DataFrame 
#   millesime : int (year)
# @return: a dictionnary with two set of features ids (unicity is guaranted)
#       "version" holds all buildings' id 
#       "versionTr" holds all transactions' id link to the current version
"""
def get_featuresid(debug,transactions, millesime):
    log(debug,f"-- Get features' ID for {millesime} --")
    ret = {"version":set(), "versionTr":set()}
    
    tr = transactions.loc[(transactions['startDate'] <= millesime) & (transactions['endDate'] > millesime)] # Filter only transactions that start in the millesime or before and end strictly after
    featureIds_at_millesime = get(debug,tr, "source", millesime)
    ret['version'].update(featureIds_at_millesime['version'])
    ret['versionTr'].update(featureIds_at_millesime['versionTr'])
    
    size_version = len(ret['version']) # Size of the versions get with the first filter
    size_versionTr = len(ret['versionTr']) # Size of the versionTr get with the first filter
    log(debug,f"simple tr : version= {size_version} - versionTR= {size_versionTr}")
    
    tr = transactions.loc[(transactions['startDate'] < millesime) & (transactions['endDate'] == millesime)] # Filter only transactions that start striclty before the millesime  and end at it
    featureIds_at_millesime = get(debug,tr, "destination", millesime)
    ret['version'].update(featureIds_at_millesime['version'])
    ret['versionTr'].update(featureIds_at_millesime['versionTr'])
    
    log(debug,f"agg tr : version= {len(ret['version']) - size_version} - versionTR= {len(ret['versionTr']) - size_versionTr}")
    
    log(debug,f"featuresIds found :  version= {len(ret['version'])} - versionTR= {len(ret['versionTr'])}")
    log(debug,f"Details : {ret}")
    return ret


"""
# Iterrate on a Dataframe to extract id of transactions for the versionTransation and id of buildings for version
#
# @Params:
#   df : DataFrame (columns=["id", "startDate", "endDate", "source", "destination", "type", "transactions"]), In use this dataFrame is a subset with only transactions that start striclty before the millesime  and end at it.
#   row_name : String row_name for the featuresId (ex: "source" or "destination") 
#   millesime : int (year)
# @return: a dictionnary {version:set, versionTr:set}
"""
def get(debug,df, row_name, millesime):
    ret = {'version':set(), 'versionTr':set()} # set for batiment's id and transactions's id to have unicity
    for index, row in df.iterrows():
        ret['versionTr'].add(row['id'])
        for s in row[row_name]:
            ret['version'].add(s) #take the source
        
        if row['transactions']==row['transactions']: # separate simple and aggregate transactions (simple has NaN value in the transactions attribut)
            for tr in row['transactions']:
                if (tr['startDate'] <= millesime and millesime < tr['endDate']) : # filter by security in the transaction aggregated
                    ret['versionTr'].add(tr['id'])
                    for s in tr['source']:
                        ret['version'].add(s)
                elif (tr['startDate'] < millesime and millesime == tr['endDate']):
                    ret['versionTr'].add(tr['id'])
                    for s in tr['destination']:
                        ret['version'].add(s)
    return ret



# Hardcoded creation of version and versionTransition
"""
# Hardcoded creation of versions and versionTransations.
# Objects of Version and VersionTransation are based on json schema for consistency with their use in py3DTiles
# @Param:
#   transactions : DataFrame
#   version_path : json schema of version
#   versionTr_path : json schema of versionTransition
# @return: (v1, v2, v3, vt_v1_v2, vt_v2_v3) (tuple) 
"""
def compile_version_and_versionTr(debug,transactions, version_path, versionTr_path):
    log(debug,"-- Compile version and versionTransition --")
    Version = 0
    VersionTransition = 0
    with open(schema_version_path) as json_file:
        version_json_data = json.load(json_file)
        Version = warlock.model_factory(version_json_data) # Create a class following the model provided by the json schema.
    
    with open(schema_versionTransition_path) as json_file:  
      versionTransition_json_data = json.load(json_file)
      VersionTransition = warlock.model_factory(versionTransition_json_data)

    v1 = Version(id="v1",
                 name="2009",
                 description="State in 2009 for the concurrent point of view",
                 startDate="2009",
                 endDate="2009",
                 tags=["concurrent"],
                 featuresIds=list(get_featuresid(debug,transactions, 2009)['version']))
    
    v2 = Version(id="v2",
                 name="2012",
                 description="State in 2012 for the concurrent point of view",
                 startDate="2012",
                 endDate="2012",
                 tags=["concurrent"],
                 featuresIds=list(get_featuresid(debug,transactions, 2012)['version']))
    
    v3 = Version(id="v3",
                 name="2015",
                 description="State in 2015 for the concurrent point of view",
                 startDate="2015",
                 endDate="2015",
                 tags=["concurrent"],
                 featuresIds=list(get_featuresid(debug,transactions, 2015)['version']))
    
    vt_v1_v2 = VersionTransition({"id":"vt1", # attribut passed by a dictionnay because "from" already meaning something. To bypasse the interpretation of it we need to pass it has a string
                                 "name":"v1->v2",
                                 "startDate":"2009",
                                 "endDate":"2012",
                                 "from":"v1",
                                 "to":"v2",
                                 "description":"Transition between v1 and v2",
                                 "type":"realized",
                                 "transactionsIds":list(get_featuresid(debug,transactions, 2009)['versionTr'])})
    
    vt_v2_v3 = VersionTransition({"id":"vt2",
                                 "name":"v2->v3",
                                 "startDate":"2012",
                                 "endDate":"2015",
                                 "from":"v2",
                                 "to":"v3",
                                 "description":"Transition between v2 and v3",
                                 "type":"realized",
                                 "transactionsIds":list(get_featuresid(debug,transactions, 2012)['versionTr'])})
    return (v1, v2, v3, vt_v1_v2, vt_v2_v3)



if __name__ == "__main__":
    # arg parse
    descr = '''This script should :
1. Load tileset.json
2. Extract information to compile versions and versionTransitions
3. Create a new tileset.json with the versions and versionTransitions data'''
    parser = argparse.ArgumentParser(description=descr)

    in_tileset_path_help = "local path for the tileset.json use in input"
    parser.add_argument('-in', '--in_path', dest='in_path', type=str, default=os.path.join('.', 'data', 'tileset.json'), help=in_tileset_path_help)
    
    out_tileset_path_help = "local path for the new tileset.json give as output"
    parser.add_argument('-out', '--out_path', dest='out_path', type=str, default=os.path.join('.', 'data', 'new_tileset.json'), help=out_tileset_path_help)
    
    schema_version_path_help = "local path for the json schema used to define the object Version"
    parser.add_argument('--schema_version_path', dest='schema_version_path', type=str, default=os.path.join('.', 'data', '3DTILES_temporal.version.schema.schema.json'), help=schema_version_path_help)
    
    schema_versionTransition_path_help = "local path for the json schema used to define the object VersionTransation"
    parser.add_argument('--schema_versionTransition_path', dest='schema_versionTransition_path', type=str, default=os.path.join('.', 'data', '3DTILES_temporal.versionTransition.schema.json'), help=schema_versionTransition_path_help)

    debug_help = "Mode debug, adds multiple print info to help know what's happenning"
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', help=debug_help)

    args = parser.parse_args()
    debug = args.debug
    if debug:
        log(debug,"Start with debug (an argument has been passed so debug is ON)")
    else:
        print("Start without debug")
        
    list_transactions = extract_transactions_from_tilesetJSON(debug,args.in_path)
    df_transactions = format_data(debug,list_transactions)
    
    schema_version_path = args.schema_version_path
    schema_versionTransition_path = args.schema_versionTransition_path

    (v1, v2, v3, vt_v1_v2, vt_v2_v3) = compile_version_and_versionTr(debug,df_transactions, 
                                                                     schema_version_path, 
                                                                     schema_versionTransition_path)
    
    with open(args.in_path) as json_file:
        data = json.load(json_file)

    data['extensions']['3DTILES_temporal']['versions'] = [v1, v2, v3]
    data['extensions']['3DTILES_temporal']['versionTransitions'] = [vt_v1_v2, vt_v2_v3]

    with open(args.out_path, "w") as json_file:
        json.dump(data, json_file)

    print('done')
