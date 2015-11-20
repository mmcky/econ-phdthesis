"""
Useful Trade Dynamics Utilities
===============================

A collection of functions useful for computing dynamic elements in trade analysis

"""


def compute_product_changes(mcp):
    """
    Compute Product Changes Year to Year

    Status: Validated

    Parameters
    ----------
    mcp : 	matrix
    		Matrix containing Country x Product Data
        
    Returns
    -------
    Dict(Products in Both Years), Dict(New Products), and Dict(Dieing Products)
    
    """
    years = mcp.keys()
    Mcp_BothYears = dict()
    Mcp_NewProducts = dict()
    Mcp_DieProducts = dict()
    # Compute t and t+1 dynamics
    for base_year in years:
        if base_year == years[-1]:
            break
        change_key = str(base_year) + '-' + str(base_year+1)
        Mcp_BothYears[change_key] = mcp[base_year] * mcp[base_year+1]
        Mcp_BothYears[change_key].name = 'ProductsBothYears'
        Mcp_NewProducts[change_key] = mcp[base_year + 1] - Mcp_BothYears[change_key]
        Mcp_NewProducts[change_key].name = 'NewProducts'
        Mcp_DieProducts[change_key] = mcp[base_year] - Mcp_BothYears[change_key]
        Mcp_DieProducts[change_key].name = 'DieProducts'
    return Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts