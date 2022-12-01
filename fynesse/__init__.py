from . import address


def predict_price(lattitude, longitude, date_str, property_type):
    return address.predict_price(date_str, lattitude, longitude, property_type)