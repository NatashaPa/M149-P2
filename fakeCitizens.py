from faker import Faker
import pymongo
import random

fake = Faker()
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["311db"]
col = db["request"]

# create fake citizen information, including unique name, phone number and address
names = [fake.unique.name() for i in range(6000)]
phone_numbers = [fake.phone_number() for i in range(3000)]
addresses = [fake.address() for i in range(6000)]


# time to insert info into fake citizen profiles
citizen_profiles = []

for i in range(6000):
    upvotes = []

    # find the request ids that will be asigned as every user's upvotes
    requests = col.find().skip(250*i).limit(random.randint(250,1000))
    for doc in requests:
        doc_id = doc["_id"]
        upvotes.append(doc_id)

        #update the request doc to include the id of the citizen that upvoted it 
        col.update_one({'_id': doc_id}, {"$push": { "upvotedByCitizensWithId": i}})        

    # citizen_profiles.append({'citizen_id': i, 'name': names[i], 'phoneNumber': phone_numbers[i], 'address': addresses[i], 'upvotedRequests': upvotes})
    citizen_profiles.append({'_id': i, 'name': names[i], 'phoneNumber': phone_numbers[random.randint(0,2999)], 'address': addresses[i], 'upvotedRequests': upvotes})


# create the citizen collection and insert the citizen profiles
db.citizens.insert_many(citizen_profiles)

# # create index on citizen_id to access faster when referencing from request collection
# db.citizens.create_index([('citizen_id', pymongo.ASCENDING)], sparse=True)

print(citizen_profiles[23])
