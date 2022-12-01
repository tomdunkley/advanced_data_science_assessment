from .config import *

from . import access
import pandas as pd

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


def data(lat, lon, date_str, bbox_size):
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df, schools, osm_data, roads = access.data(lat, lon, bbox_size, date_str)

    #### ADD ONE-HOT FOR ROAD TYPES
    road_types = list(df["road_type"].unique())
    # Make first road type the default value to whichh other roads are compared
    road_types.pop(0)

    print(road_types)

    # Add one-hot column for each other possible road type
    for road_type in road_types:
        df[road_type] = (df["road_type"]==road_type).astype(int)




    #### ADD ONE-HOT FOR PROPERTY TYPES
    property_types = list(df["property_type"].unique())
    # Make the first element the default value to which other types are compared
    property_types.pop(0) 

    print(property_types)

    # Add one-hot column for each other possible property type
    for property_type in property_types:
        df[property_type] = (df["property_type"]==property_type).astype(int)



    #### CLEAN UP DATE
    df["clean_date"] = (pd.to_datetime(df["date_of_transfer"]).astype(int) / (10**9 * 86400)).astype(int)



    ## REMOVE UNWANTED COLUMNS
    df = df[["clean_date", "distance_to_nearest_school", "nearby_building_count"] 
        + road_types + property_types + ["price"]]
    
    return df, property_types, road_types, schools, osm_data, roads

