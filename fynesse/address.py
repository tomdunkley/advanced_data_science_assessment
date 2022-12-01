# This file contains code for suporting addressing questions in the data

from . import access, assess

import datetime, time
import numpy as np
import statsmodels.api as sm

"""Address a particular question that arises from the data"""
def predict_price(date_str, lattitude, longitude, property_type, bbox_size=0.01):
    df, property_types, road_types, schools, osm_data, roads = assess.data(lattitude, longitude, date_str, bbox_size)
    school_distance = access.distance_to_nearest_school(lattitude, longitude, schools)
    nearby_building_count = access.building_count(lattitude, longitude, osm_data)
    road_type = access.get_nearest_road_type(lattitude, longitude, roads)
    params = [1, clean_date(date_str), school_distance, nearby_building_count]
    params += [1 if road_type == r else 0 for r in road_types]
    params += [1 if property_type == p else 0 for p in property_types]
    print(params)

    results = create_model(df)

    return results.predict(params)[0]


def create_model(df):
    y = np.array(df["price"], dtype=float)
    xs = np.array(df.drop("price", axis=1), dtype=float)
    xs = sm.add_constant(xs, prepend=True)
    m_linear = sm.OLS(y,xs)
    return m_linear.fit()

def clean_date(date_str):
  return time.mktime(datetime.datetime.strptime(date_str, "%Y-%m-%d").timetuple()) / (86400)