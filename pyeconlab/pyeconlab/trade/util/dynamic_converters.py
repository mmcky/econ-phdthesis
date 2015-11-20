"""
Pandas Dynamic Converters
"""

import pandas as pd
import copy
import numpy as np

def compute_persistence(mcp, dict_cp, persistence_length=1, name="Var", output="summary", verbose=False):
    ''' Compute Persistence in the Data ... Ensure Product Persists in n-folowing years
        Status: Validated (May Consider different Versions to improve speed)
        Return:     persisent_data
        Options:    persistence_length  Defines how many years to look forward (default=1 year)
                    name                IF dataframe has no name it will be assigned Var and returned VarPersistent
                    output              'summary' -> returns sum (across products) Country Level Data else returns Country x Product Level
    '''
    #Copy of Data to Modify
    persistent_data = copy.deepcopy(dict_cp)
    #Derive Year Data
    start_year = dict_cp.keys()[0]
    num_years = len(dict_cp)
    final_year = start_year+num_years-1
    #Create Null Matrix of Pane Size (Based on Initial Year)
    null_matrix = np.empty(shape=(dict_cp[start_year].shape))
    null_matrix[:] = np.nan
    #Caputure DataName if there is One
    try:
        data_name = dict_cp[start_year].name
    except:
        data_name = name
    for year in persistent_data.keys():
        for i in range(1,persistence_length+1,1):
            if (year+i) > final_year:
                persistent_data[year] = pd.DataFrame(null_matrix, index=persistent_data[year].index, columns=persistent_data[year].columns)
            else:
                persistent_data[year] = persistent_data[year] * mcp[year+i]
        persistent_data[year].name = data_name + 'Persistent'
        if output.lower() == "summary":
            persistent_data[year] = persistent_data[year].sum(axis=1)
    return persistent_data

def reindex_dynamic_dataframe(df, year_pairs='years', verbose=False):
    ''' Converts a dynamicaly referenced Dataframe (i.e. '1962-1963') to a dataframe with index to_year and from_year
    '''
    new_index = []
    for item in df.index:
        (years, country, productcode) = item
        from_year = years.split('-')[0]
        to_year = years.split('-')[1]
        new_index.append((int(from_year), int(to_year), country, productcode))
    new_index = pd.MultiIndex.from_tuples(new_index, names=['from_year', 'to_year', 'country', 'productcode'])
    new_df = df.copy()
    new_df.index = new_index
    return new_df

def reindex_dynamic_dict(data_dict, base='start', verbose=False):
    ''' Converts a Dynamic Dict (Year-Year) to be indexed by either start or finish year
        Options:    base    start -> Reindexes Dictionary by Start Year
                            finish -> Reindexes Dictionary by Finish Year
    ''' 
    new_dict = dict()
    for key in data_dict.keys():
        start_year = int(key.split('-')[0])
        finish_year = int(key.split('-')[1])
        if verbose: print "Key: %s, Start_Year: %s, Finish_Year: %s" % (key, start_year, finish_year)
        if str(base).lower() == 'start':
            new_dict[start_year] = data_dict[key]
        elif str(base).lower() == 'finish':
            new_dict[finish_year] = data_dict[key]
        else:
            print "Error: base needs to be 'start' year or 'finish' year"
    return new_dict