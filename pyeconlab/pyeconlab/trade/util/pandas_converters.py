"""
Converter Utilities for Pandas Objects 
DataFrames and Panels
"""

import pandas as pd

###-----------------------###
###---TO PANDAS OBJECTS---###
###-----------------------###


def from_dict_to_dataframe(dictionary, matrix_shape='cp', out_shape='long', verbose=False):
    """ 
    Converts a Dict(DataFrame) to a DataFrame with Heirachical Index
    matrix Shape:   'cp' -> Rows: Country x Columns: ProductCode
                    'pp' -> Rows: ProductCode x Columns: ProductCode
                    'c'  -> Rows: Country (with 1 Series); Currently enforces out_shape to be long
                    'p'  -> Rows: Products (with 1 Series); Currently enforces out_shape to be long
    """
    if len(matrix_shape) == 2:
        panel = from_dict_to_panel(dictionary, verbose)
        data_name = panel.name
        dataframe = panel.to_frame(filter_observations=False)                                                            #11/05/2013 - Added: 'filter_observations=False' due to NaN panels in rca computation
        dataframe.name = data_name
    elif len(matrix_shape) == 1:
        dataframe = pd.DataFrame(dictionary)
        data_name = dictionary[dictionary.keys()[0]].name
    else:
        print "ERROR: matrix_shape must be 'cp', 'pp', 'c', 'p'"
    if verbose: print "Note that to_frame hasn't preserved index.names %s" % dataframe.index.names                   # Note this hasn't preserved the name 'productcode in conversion'!
    if matrix_shape.lower() == 'cp':
        dataframe.index.names = ['country', 'productcode']  
        out_index_order = ['year', 'country', 'productcode']
    elif matrix_shape.lower() == 'pp':
        dataframe.index.names = ['productcode1', 'productcode2']
        out_index_order = ['year', 'productcode1', 'productcode2']
    elif matrix_shape.lower() == 'c':
        dataframe.index.names = ['country']
        out_index_order = ['year', 'country']
        out_shape = 'long'
    elif matrix_shape.lower() == 'p':
        dataframe.index.names = ['productcode']
        out_index_order = ['year', 'productcode']
        out_shape = 'long'
    dataframe = dataframe.stack()
    dataframe.name = data_name
    dataframe = pd.DataFrame(dataframe)         #Stack returns a Series - but want to keep in DataFrame
    #dataframe.index.names[-1] = 'year'         #0.13 Issue -> introduction of Immutable Metadata and include set_names method
    names = list(dataframe.index.names)
    names[-1] = 'year'
    dataframe.index.set_names(names, inplace=True)
    #Format Long or Wide Format
    if out_shape.lower() == 'long':
        if verbose: print 'Out Index Order: %s' % out_index_order
        dataframe = dataframe.reorder_levels(out_index_order)
    elif out_shape.lower() == 'wide':
        if matrix_shape.lower() == 'cp': dataframe = dataframe.unstack('productcode')
        if matrix_shape.lower() == 'pp': dataframe = dataframe.unstack('productcode2')
        if verbose: print 'Out Index Order: %s' % out_index_order
        dataframe = dataframe.reorder_levels(out_index_order[0:2])
    else:
        print 'out_shape: Required to be long or wide'
    return dataframe


def from_dict_to_panel(dictionary, minor_axis='productcode', verbose=False):
    ''' Converts a Dict(pd.DataFrame) (Dict Key: Years) to a pd.Panel
    '''
    #Assume Dictionary Contains all of the SAME TYPE of Information
    years = dictionary.keys()
    panel = pd.Panel(dictionary)
    panel.name = dictionary[years[0]].name
    panel.items.name = 'year'
    panel.minor_axis.name = minor_axis
    return panel

def from_dict_of_series_to(dictionary, dict_key='year', series_name='Unknown', out_type='dataframe', verbose=False):
    ''' Converts a Dict(Series) to a Series with Heirarchical Index
        Status: Phasing-Out (Soon Deprecated due to adding dict(series) to from_dict_to_dataframe)
        Note:           Currently only 'Year' Key has been implimented so that the data_series can be labelled correctly
        Future Work:    Bring Series_name in from data that is already labelled. 
    '''
    
    if dict_key.lower() == 'year':
        df = pd.DataFrame(dictionary)
        se = df.unstack()
        index_name = se.index.names[1]
        se.index.set_names(['year', index_name], inplace=True)
        se.name = series_name                                                   #Could improve this to account for incoming named data
        se = se.reorder_levels([index_name, 'year']).sortlevel()    
    if out_type.lower() == 'series':
        return se
    elif out_type.lower() == 'dataframe':
        df = pd.DataFrame(se)
        return df
    else:
        print "out_type must be specified as 'series' or 'dataframe'"


###-------------------------###
###---FROM PANDAS OBJECTS---###
###-------------------------###

def from_long_to_dict(panel_long, out_shape='wide', shape='cp', verbose=False):
    ''' Converts from a Long DataFrame to A Dict of Years (DataFrame)
        Status: In-TESTING
        Changes: 01/10/2013     -> Updated to propogate panel_long.name attribute to all dict() slices
    '''
    try:
        data_name = panel_long.name
    except:
        print "Warning: Long Panel has No Name"
    if type(panel_long) == pd.Series:
        panel_long = pd.DataFrame(panel_long)
    elif len(panel_long.columns) > 1:
        print "Warning: MUST RETURN LONG as there are multiple Series. Do not want to return Heirachical Columns in Dictionary as it makes referencing harder [Setting out_shape = 'long']"  
        out_shape='long'
    data_dict = dict()
    if shape =='cp':
        panel_long = panel_long.reorder_levels(order=['year', 'country', 'productcode'])
    elif shape == 'pp':
        panel_long = panel_long.reorder_levels(order=['year', 'productcode1', 'productcode2'])
    else:
        print "ERROR: shape option must be 'cp' or 'pp'"
    att = return_data_attributes(panel_long)
    for year in att['year']:
        data_dict[year] = panel_long.ix[year]
        if out_shape=='wide': 
            if shape == 'cp': 
                data_name = panel_long.columns[0]
                data_dict[year] = data_dict[year].unstack(level='productcode')[data_name]
                data_dict[year].name = data_name
            elif shape == 'pp':
                data_name = panel_long.columns[0] 
                data_dict[year] = data_dict[year].unstack(level='productcode2')[data_name]
                data_dict[year].name = data_name
            else: 
                print "ERROR: Shape must be cp or pp"
        data_dict[year].name = data_name
    return data_dict

###---------------------###
###---Reindex Methods---###
###---------------------###

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

def reindex_multi_to_single(df):
    df.index = pd.Index(df.index)
    return df

def reindex_single_to_multi(df):
    df.index = pd.MultiIndex.from_tuples(df.index)
    return df