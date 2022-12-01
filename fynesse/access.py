from .config import *

import osmnx, geopandas, yaml
import pandas as pd
from sqlalchemy.engine import create_engine
from shapely.geometry import Point


# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data(lat, lon, bbox_size, date_str):
    """Read the data from the web or local file, returning structured format such as a data frame"""
    max_lat = float(lat)+float(bbox_size)
    min_lat = float(lat)-float(bbox_size)
    max_lon = float(lon)+float(bbox_size)
    min_lon = float(lon)-float(bbox_size)
    min_year = str(int(date_str[:4]) - 1) + date_str[4:]
    max_year = str(int(date_str[:4]) + 1) + date_str[4:]
    df = getPostcodesWithinBbox(min_lat, max_lat, min_lon, max_lon, min_year, max_year, create_database_connection())

    # set geometry column
    df["geometry"] = geopandas.points_from_xy(df.longitude, df.lattitude)

    # convert to geodataframe
    df = geopandas.GeoDataFrame(df)

    # limit postcodes to those inside the circle that inscribes the above rectangle (i.e those within bbox_size of (lat, lon))
    df = df[df.geometry.distance(Point(lon,lat)) < bbox_size]

    query_limit = bbox_size*111*1000 # bbox converted to metres (ish)
    osm_data = osmnx.geometries_from_point((lat,lon), dist=query_limit, tags={"building":True, "highway":True, "amenity":"school"}) 

    # If "school" not in keys then there is no school within the bounding box, so we set that column to all zeros.
    if "school:type" in osm_data.keys():
        schools = osm_data[osm_data["school:type"].isin(["academy", "community", "free", "voluntary_aided", "voluntary_controlled"])]
    if len(schools) > 0:
        df["distance_to_nearest_school"] = df.apply(lambda row: distance_to_nearest_school(float(row["lattitude"]), float(row["longitude"]), schools), axis=1)
    else:
        df["distance_to_nearest_school"] = df["lattitude"]*0

    # If "building" not in keys then there is no building within the bounding box, so we set that column to all zeros.
    if "building" in osm_data.keys():
        df["nearby_building_count"] = df.apply(lambda row: building_count(float(row["lattitude"]), float(row["longitude"]), osm_data), axis=1)
    else:
        df["nearby_building_count"] = df["lattitude"]*0


    roads = osm_data[(osm_data["highway"]==osm_data["highway"]) & (osm_data.geom_type=="LineString")] # get only highways (not nans) that are "ways"

    df["road_type"] = df.apply(lambda row: get_nearest_road_type(float(row["lattitude"]), float(row["longitude"]), roads), axis=1) 


    return df, schools, osm_data, roads


def create_database_connection():
    """Create a connection to the database and return it"""
    database_details = {"url": "database-td457.cgrre17yxw11.eu-west-2.rds.amazonaws.com", 
                    "port": 3306}

    # get credentials
    with open("credentials.yaml", "w") as file:
        credentials_dict = {'username': input("Username: "), 
                            'password': input("Password: ")}
        yaml.dump(credentials_dict, file)

    with open("credentials.yaml") as file:
        credentials = yaml.safe_load(file)
    username = credentials["username"]
    password = credentials["password"]
    url = database_details["url"]

    url = database_details["url"]

    return create_engine("mariadb+pymysql://{}:{}@{}?local_infile=1".format(username, password, url))



def getPostcodesWithinBbox(min_lat, max_lat, min_lon, max_lon, max_year, min_year, conn):
    """Get all of the postcodes within the described box"""
    conn.execute("USE property_prices;")
    return pd.read_sql("SELECT postcode, lattitude, longitude FROM postcode_data WHERE lattitude < {} AND   lattitude > {} AND   longitude < {} AND   longitude > {} AND pp_data.date_of_transfer <= {} AND pp_data.date_of_transfer >= {};".format(max_lat, min_lat, max_lon, min_lon, max_year, min_year), conn)

# Create new distance function with schools as an argument
def distance_to_nearest_school(lat, lon, schools):
  home = Point(lon, lat) # set home point
  schools["distance_from_home"] = schools["geometry"].distance(home) # get distance to home point for each school
  return schools["distance_from_home"].min() # find and return minimum distance


def building_count(lat,lon,osm_data,limit=0.0005):
  home = Point(lon, lat)
  buildings = osm_data[osm_data["building"]==osm_data["building"]] # remove NaNs
  buildings["distance_from_home"] = buildings["geometry"].distance(home)
  return sum(buildings["distance_from_home"] < limit)

def get_nearest_road_type(lat,lon,roads):
  home = Point(lon, lat) # set home point
  roads["distance_from_home"] = roads["geometry"].distance(home) # get distance to home point for each school
  return roads[roads["distance_from_home"]==roads["distance_from_home"].min()]["highway"][0]


