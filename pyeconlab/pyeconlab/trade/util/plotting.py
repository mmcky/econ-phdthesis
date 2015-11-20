"""
Ploting Utilities
"""

import pandas as pd
import numpy as np
from matplotlib import cm
import matplotlib.pyplot as plt

def prepare_scaling_vectors(cpdata, row_scaleby=None, column_scaleby=None):
	"""
	Prepares scaleby vectors for plot_scaled_mcp_heatmap() function 
	[Basically generates a jointly indexed set of values for rowsortby and rowscale by vectors]

	Notes
	-----
		1. Only data matching between the original cpdata matrix and the scaleby series will be returned
			This allows one to use country_income vectors and any extra data is discarded in preparing the scalebys 

	Work
	----
		1. Where should this method live? Could be useful for other functions
	"""
	if type(row_scaleby) == pd.Series:
		rownames = cpdata.index
		row_scaleby = row_scaleby[rownames]                 #Select Appropriate Data
		row_scaleby = row_scaleby[row_scaleby.notnull()]
		sorted_df_year = cpdata.ix[row_scaleby.index]       #Get the Matching Data
	if type(column_scaleby) == pd.Series:
		colnames = cpdata.columns
		column_scaleby = column_scaleby[colnames]
		column_scaleby = column_scaleby[column_scaleby.notnull()]
		sorted_df_year = cpdata[column_scaleby.index]
	# - Returns relevant scaleby vectors - #
	return cpdata, row_scaleby, column_scaleby

#-->Tidy Up Below<--#

def prepare_scaling_sortby_vectors(cpdata, row_scaleby=None, column_scaleby=None):
	return prepare_scaling_vectors(cpdata, row_scaleby=row_scaleby, column_scaleby=column_scaleby)

def remove_zero_relationships(dataframe_matrix, over_index='both', verbose=False):
    ''' Remove Rows and Columns that contain ALL ZERO relationships between Index 1 and Index 2
        Status: IN-DEVELOPMENT
        Input: DataFrame (Matrix Format). ONLY WIDE DATA
        Return: adjusted DataFrame
        Note:   1. Symmetric Square Matrix Assumption - I could drop half the computation in this function, but I will keep it general for now (Best Keep it General in case not Symmetric!)
                2. Could simply this by removing duplicate code between both and rows and columns 
    '''
    if str(over_index).lower() == 'both':
        col_sum = dataframe_matrix.sum(axis=0)     #pd.Series
        if verbose: print 'Removing %s Columns: %s' % (len(col_sum[col_sum==0].index), col_sum[col_sum==0].index)
        row_sum = dataframe_matrix.sum(axis=1)     #pd.Series
        if verbose: print 'Removing %s Rows: %s' % (len(row_sum[row_sum==0].index), row_sum[row_sum==0].index)
        col_labels = list(col_sum[col_sum == 0].index)  
        row_labels = list(row_sum[row_sum == 0].index)
        row_name = dataframe_matrix.index.name
        col_name = dataframe_matrix.columns.name
        updated_matrix = dataframe_matrix.drop(col_labels, axis=1)                                     #In Symmetric Square Matrix These are the SAME list of Labels!
        updated_matrix = updated_matrix.drop(row_labels, axis=0)
        updated_matrix.index.name = row_name
        updated_matrix.columns.name = col_name                                                 
        new_dataframe_matrix = updated_matrix
        return new_dataframe_matrix
    elif str(over_index).lower() == 'rows':                           #Perform Operation over Rows Only 
        row_sum = dataframe_matrix.sum(axis=1)     #pd.Series
        if verbose: print 'Removing %s Rows: %s' % (len(row_sum[row_sum==0].index), row_sum[row_sum==0].index)
        row_labels = list(row_sum[row_sum == 0].index)
        row_name = dataframe_matrix.index.name
        updated_matrix = dataframe_matrix.drop(row_labels, axis=0)
        updated_matrix.index.name = row_name
        return updated_matrix
    elif str(over_index).lower() == 'columns':                                #Perform Operation over Columns Only
        col_sum = dataframe_matrix.sum(axis=0)     #pd.Series
        if verbose: print 'Removing %s Columns: %s' % (len(col_sum[col_sum==0].index), col_sum[col_sum==0].index)
        col_labels = list(col_sum[col_sum == 0].index)
        col_name = dataframe_matrix.columns.name
        updated_matrix = dataframe_matrix.drop(col_labels, axis=1)
        updated_matrix.columns.name = col_name
        return updated_matrix
    else:
        print "ERROR: over_index must be 'both', 'columns', or 'rows!"
        return None

default_cntrys = ['JPN', 'USA', 'DEU', 'KOR', 'ITA', 'AUS', 'BRA', 'THA', 'GHA', 'GBR', 'TWN', 'FRA', 'MEX']
default_prods = ['8421', '3330', '7922', '8748', '7810', '7442', '7781']
default_row_labels = ('ECI', 'Export Value Share')
default_column_labels = ('PCI', 'Product Value Share')
def plot_scaled_mcp_heatmap_v3(sorted_data_year, row_scaleby='', column_scaleby='', year=2000, row_label=default_row_labels, label_cntrys=default_cntrys, column_label=default_column_labels, label_prods=default_prods, value_type='RCA', low_value_cutoff=np.nan, high_value_cutoff=-1, size=8, axs_only=False, cmap=cm.Reds, verbose=False):
    """ 
    Plot Mcp, RCA, Export, ExportShare etc. to be graphed as a scaled heatmap pcolor chart!)
    
    Status:   IN-DEVELOPMENT
    
    Parameters
    ----------
    sorted_data_year 
    row_scaleby     -> Specify a row_scaleby series (i.e. GDP or Country Exports etc.)
    column_scaleby  -> Specify a column_scaleby series
    year            -> Used in Title
    row_label       -> Text for Row (or Y Axis) Type('row_sortby', 'row_scaleby')
    label_cntrys    -> Specify which countries to label
    column_label    -> Text for Column (or X Axis)
    label_prods     -> Specify which products to label
    low_value_cutoff -> Specify Low Value to Cutoff (Defaults to 0 if not specified explicitly) [Works in Most Use Cases Except Percent Share Data]
    value_cutoff    -> Specify value cutoff for heatmap color variation 
    size            -> Size of Heatmap Figure
    axs_only        -> Allows axs to be an input and return graph axs only. (Useful when compiling evolution of graph via Kpn and Kcn)

    Dependancies
    ------------
    	1. prepare_scaling_sortby_vectors()

    Notes
    -----
    	1. Version 3 incorporates option to change ColorMap and Allows for Negative Numbers
    """

    def index_array(labels, items):
        ''' Take np.array and return appropriate index positions for items'''
        index = []
        for item in items:
            try:
                idx = np.where(labels==item)
                index.append(idx[0][0])        #First [0] extracts from tuple, Second[0] extracts from ndarray
            except:
                print "WARNING: %s is not found in sortby or scaleby vectors. Ignoring!" % item
                continue 
        return np.array(index)

    #Print Reminders
    print "Note: Value Type is: %s; Make Sure you have set appropriate high_value_cutoff: %s for optimal views" % (value_type, high_value_cutoff)
    print "WARNING: This function defaults to naming rows, Sortby: %s, Scaleby: %s" % default_row_labels
    print "WARNING: This function defaults to naming columns, Sortby: %s, Scaleby: %s" % default_column_labels

    #Pandas Work
    data = remove_zero_relationships(sorted_data_year)
    countries = data.index
    products = data.columns
    '''
    if low_value_cutoff == np.nan:                  #Default Set for RCA Filter
        low_value_cutoff = 1.0
    if high_value_cutoff != -1:
        matr = data.applymap(lambda x: 0 if x < low_value_cutoff else x).applymap(lambda x: high_value_cutoff if x >= value_cutoff else x).as_matrix()        
        value_note = value_type + ' Gradient Cutoff >= ' + str(high_value_cutoff)
    else:
    '''
    if verbose: print "All Plot Values will be shown. To make Graph clearer use: value_cutoff and low_value_cutoff options"
    matr = data.as_matrix()
    rca_note = ''
    #Scaling Vectors
    if type(row_scaleby) == pd.Series:
        yscale = np.array(row_scaleby)
        yscale = yscale / np.sum(yscale)                    #Normalise (0,1) 
        yscale = np.insert(yscale.cumsum(), 0, 0)           #Replace y with scaled vector
        ylabel = np.array(countries)
    else: 
        y = np.array(countries)
        yscale = np.arange(len(y))
        ylabel = np.array(countries)
    if type(column_scaleby) == pd.Series: 
        xscale = np.array(column_scaleby)
        xscale = xscale / np.sum(xscale)                    #Normalise (0,1)
        xscale = np.insert(xscale.cumsum(), 0, 0)       #Replace x with scaled vector
        xlabel = np.array(products)                 
    else:
        x = np.array(products)
        xscale = np.arange(len(x))
        xlabel = np.array(products)
    xx,yy = np.meshgrid(xscale,yscale)
    #Matplotlib Work
    if axs_only != False:
        axs = axs_only
    else:
        fig = plt.figure(figsize=(size*1.2,size))                           #The 1.2 multiple allows for the incorporation of a colorbar
        axs = fig.add_subplot(1,1,1)
    plot = axs.pcolormesh(xx,yy,matr, cmap=cmap)                         #Could Consider making cmap an option (but not implimented b/c Reds works well and all graphs will then be standardized)
    if type(row_scaleby) and type(column_scaleby) == pd.Series:
        axs.set_ylim(0,1)
        axs.set_xlim(0,1)
    elif type(row_scaleby) == pd.Series:
        axs.set_ylim(0,1)
        axs.set_xlim(0, len(x))
    elif type(column_scaleby) == pd.Series:
        axs.set_ylim(0, len(y))
        axs.set_xlim(0,1)
    else:
        axs.set_ylim(0, len(y))
        axs.set_xlim(0, len(x))
    #Labeling Axes
    idx = index_array(ylabel, label_cntrys)
    centre_adj = (yscale[idx+1] - yscale[idx]) /2
    axs.set_yticks(yscale[idx] + centre_adj)
    axs.set_yticklabels(ylabel[idx])
    idx = index_array(xlabel, label_prods)
    centre_adj = (xscale[idx+1] - xscale[idx]) /2
    axs.set_xticks(xscale[idx] + centre_adj)
    axs.set_xticklabels(xlabel[idx])
    if axs_only != False:
        return axs
    #Colorbar
    if len(str(matr.max())) > 4:                       #For Long Values (like Exports) Put Value on New Line, This is a fairly arbitrary decision re: length so may consider turning this into a flag. 
        if matr.min() < 0:                             #Formatting for Lower Bounds that are Negative
            cbar = plt.colorbar(plot, ticks=[matr.min(), (matr.max() - abs(matr.min()))/2, matr.max()])
            cbar.set_ticklabels(['<= %0.0e' % (matr.min()), '%0.0e' % ((matr.max() - abs(matr.min()))/2), '>= %0.0e' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
            cbar.ax.set_ylabel(value_type)
        else:
            cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
            cbar.set_ticklabels(['<= %0.0e' % ((matr.min()), '%0.0e' % (matr.max())/2), '>= %0.0e' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
            cbar.ax.set_ylabel(value_type)
    else:
        cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
        cbar.set_ticklabels(['<= %0.2f' % (matr.min()), '= %0.2f' % (matr.max()/2), '>= %0.2f' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
        cbar.ax.set_ylabel(value_type)
    #Figure Text
    figtitle = "Mcp [" + value_type + " Values] Matrix [Yr: " + str(year) + "]"
    plt.title(figtitle)
    sortby, scaleby = row_label
    ylabel = 'Countries [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
    axs.set_ylabel(ylabel)
    sortby, scaleby = column_label
    xlabel = 'Products [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
    axs.set_xlabel(xlabel)
    #Notes
    note = 'Countries: ' + str(len(countries)) + '; Products: ' + str(len(products))
    plt.figtext(0.7, 0.01, note)
    plt.tight_layout()
    return fig