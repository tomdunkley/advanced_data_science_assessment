from . import address


def predict_price(date_str, lattitude, longitude, property_type):
    return address.predict_price(date_str, lattitude, longitude, property_type)