from faker import Faker
import pymongo
import random
import pprint

fake = Faker()
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["311db"]
col = db["request"]

# keep all requests in a variable
requests = col.find()

# iterate over all requests
for r in requests:

    r_id = r["_id"]

    if 'latitude' in r["location"].keys():
    
        # store latitide and longitude in variables
        lat = r["location"]['latitude']
        lon = r["location"]['longitude']
        
        if lat != '':
            f_lat = float(lat)      
            updatedLocationField = { "$set": { "location": {'type': 'Point', 'coordinates': [float(lat), float(lon)]} } }

            col.update_one({'_id': r_id}, updatedLocationField)
