import pandas as pd
import networkx as nx
import numpy as np

from .dynamics import compute_product_changes

def compute_average_centrality(mcp, proximity, normalized=True, sum_not_mean=False):
    """
    Compute Average Centrality from Mcp and Proximity matrices
        
    Parameters
    ----------
    normalized  :   bool, optional(default=True)
                    Normalize by the Total Number of Products; if False the denominator is the number of products exported by that country
    sum_not_mean :  bool, optional(default=False)
                    Sum's the mean proximity multiplied by country export basket

    Notes
    ----- 
        1. sum_not_mean = Haussman uses SUM() rather than normalised mean -> Same Overall Graph Shape!Return: Dict()
    
    """
    if type(mcp) == dict and type(proximity == dict):
        avg_centrality = dict()
        years = mcp.keys()                          #should this be the SET of KEYS for both mcp and proximity?    
        for year in years:
            if normalized:
                avg_centrality[year] = mcp[year].mul(proximity[year].mean(), axis=1).mean(axis=1)
                if sum_not_mean: avg_centrality[year] = mcp[year].mul(proximity[year].mean(), axis=1).sum(axis=1)       #05/07/2013 -> change from mcp. to mcp[year].
            else:
                centrality = proximity[year].sum() / len(proximity[year])                                               # Explicit FORM; num_products = len(proximity[year])
                country_centrality = centrality * mcp[year]
                num_prods_exported = mcp[year].sum(axis=1) 
                avg_centrality[year] = (country_centrality.sum(axis=1) / num_prods_exported)
        return avg_centrality
    else:
        if normalized: 
            avg_centrality = mcp.mul(proximity.mean(), axis=1).mean(axis=1)
            if sum_not_mean: avg_centrality = mcp.mul(proximity.mean(), axis=1).sum(axis=1)                                    
        else:
            num_prods_exported = mcp.sum(axis=1)                                                                        # Condensed FORM
            avg_centrality =  mcp.mul(proximity.mean(), axis=1).sum(axis=1).div(num_prods_exported)
    return avg_centrality


def compute_diffusion_properties_nx(mcp, proximity,  style='average', verbose=False):
    """ 
    Compute the diffusion properties AvgProx, VarProx, and WidthProx in a single pass through the data (to improve speed)
    
    Status: IN-WORK
    
    Parameters
    ----------
    mcp    :    pd.DataFrame(Mcp)
                Mcp (CP Matrix of Country Export's w/ RCA)
    proximity : pd.DataFrame(Proximity)
                Meausure of relative product similarity and difference
    style   :   str, optional(default='average')
                Specify how to treat two proximity values ['base', 'next', 'average']
    
    Returns
    -------
    Mc_WidthDiffusion, Mc_VarianceDiffusion, Mc_AverageDiffusion 

    """
    #Initialise Return Structures
    Mc_AverageDiffusion = dict()
    Mc_VarianceDiffusion = dict()
    Mc_WidthDiffusion = dict()
    Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = compute_product_changes(mcp)
    for years in sorted(Mcp_NewProducts.keys()):
        base_year, next_year = [int(x) for x in years.split('-')]
        if verbose: print "Years: %s; Base_Year: %s, Next_Year: %s" % (years, base_year, next_year)
        Prox_BaseYear = proximity[base_year]
        Prox_NextYear = proximity[next_year]
        #Cross_Section
        countries = Mcp_BothYears[years].index
        products = Mcp_BothYears[years].columns 
        AvgProx = pd.DataFrame(np.zeros((len(countries), 1)),  index=countries, columns=['AvgProx'])
        VarProx = pd.DataFrame(np.zeros((len(countries), 1)),  index=countries, columns=['VarProx'])
        WidthProx = pd.DataFrame(np.zeros((len(countries), 1)),  index=countries, columns=['WidthProx'])
        #Load Network
        if style.lower() == 'base':
            prox = Prox_BaseYear
        elif style.lower() == 'next':
            prox = Prox_NextYear
        elif style.lower() == 'average':
            prox = (Prox_BaseYear+Prox_NextYear) / 2.0
        else:
            print "ERROR: Need to Specify style as: 'base', 'next', or 'average'"
            return None
        prox.name = 'proximity'
        network = construct_network_from_adjacency_df(prox)   #Currently chose base_year but need to create the right dataframe during the prox_cutoff stage
        for country in Mcp_NewProducts[years].index:            #Iterate through Countries in NewProducts
            last_year_exports = pd.DataFrame(mcp[base_year].ix[country])  #DataFrame Vector of Last Years Exports
            sum_prox = 0.0
            num_prox = 0.0
            connection_list = []
            for product in Mcp_NewProducts[years].columns:      #Iterate through Products  in NewProducts
                if Mcp_NewProducts[years].get_value(index=country, col=product) == 1:
                    for prev_product in last_year_exports.iterrows():
                        if prev_product[1][country] == 1.0:
                            proximity_val = network.get_edge_data(product, prev_product[0])['weight']  #Provides Proximity Between New Product and Previous Product
                            #AvgData
                            num_prox += 1
                            sum_prox += proximity_val
                            #Var and Width
                            connection_list.append(proximity_val)
            #AvgData
            if num_prox == 0.0:
                continue            #This can happen if there are no new products in a given year
            else:
                avg_prox = sum_prox / num_prox
                AvgProx.set_value(index=country, col='AvgProx', value=avg_prox)
            #Variance
            variance = np.var(connection_list)
            VarProx.set_value(index=country, col='VarProx', value=variance)
            #WidthConnection
            try:
                width = np.ptp(connection_list)
            except:
                width = np.nan
            WidthProx.set_value(index=country, col='WidthProx', value=width)
        Mc_AverageDiffusion[next_year] = AvgProx['AvgProx']   #Return Series to work with from_dict_of_series_to() function
        Mc_AverageDiffusion[next_year].name = 'AvgProx'
        Mc_VarianceDiffusion[next_year] = VarProx['VarProx']   
        Mc_VarianceDiffusion[next_year].name = 'VarProx'
        Mc_WidthDiffusion[next_year] = WidthProx['WidthProx']   
        Mc_WidthDiffusion[next_year].name = 'WidthProx'
    return Mc_AverageDiffusion, Mc_VarianceDiffusion, Mc_WidthDiffusion

def construct_network_from_adjacency_df(data):
    """ 
    Construct a Network Using Networkx from an Adjacency Matrix

    Parameters
    ----------
    data    :   pd.DataFrame(CP and PP Matrix)
                Adjacency Matrix (i.e. cp or pp matrix) as a Pandas DataFrame
    
    """   
    if type(data) == type(pd.DataFrame()):
        data_name = data.name
        data = pd.DataFrame(data.unstack())             
        data.columns = pd.Index([data_name])
        data = data.reset_index()                   #Move Index to Data Level to Parse as a Weighted Edge List
        network = nx.Graph()
        for row in data.iterrows():
            index, row_data = row
            source = row_data['productcode1']
            target = row_data['productcode2']
            edge_weight = row_data[data_name]
            network.add_edge(source, target, weight=edge_weight)
    return network