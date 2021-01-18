from flask import Flask, request, jsonify, render_template, redirect
from flask_pymongo import PyMongo
from bson.objectid import ObjectId
import datetime 

app = Flask(__name__)
app.config["MONGO_URI"] = "mongodb://localhost:27017/311db"
mongo = PyMongo(app)

@app.route('/', methods = ['GET'])
def hello_world():
    return render_template("home.html") 

@app.route('/create_incident', methods = ['POST'])
def create_incident():

    try:
        incident = {
            'creationDate': datetime.datetime.now(),
            'status': 'Open',            
            'serviceRequestNumber': request.form['serviceRequestNumber'],
            'serviceRequestType': request.form['serviceRequestType'],
            'streetAddress': request.form['streetAddress'],
            'zipCode': int(request.form['zipCode']),
            'xCoordinate': float(request.form['xCoordinate']),
            'yCoordinate': float(request.form['yCoordinate']),
            'ward': int(request.form['ward']),
            'policeDistrict': int(request.form['policeDistrict']),
            'communityArea': int(request.form['communityArea']),            
            'location': {
                'type': 'Point',
                'coordinates': [float(request.form['lat']), float(request.form['lon'])]},
            'upvotedByCitizensWithId': []              
        }

        if str(request.form['serviceRequestType'])== 'Abandoned Vehicle Complaint':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['licensePlate'] = request.form['licensePlate']
            incident['typeInfo']['vehicleModel'] = request.form['vehicleModel']
            incident['typeInfo']['vehicleColor'] = request.form['vehicleColor']
            incident['typeInfo']['howManyDaysHasTheVehicleBeenReportedAsParked?'] = int(request.form['howManyDays'])

        elif str(request.form['serviceRequestType'])== 'Garbage Cart Black Maintenance/Replacement':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['numberOfBlackCartsDelivered'] = int(request.form['carts'])

        elif str(request.form['serviceRequestType'])== 'Graffiti Removal':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['whatTypeOfSurfaceIsTheGraffitiOn'] = request.form['gsurface']
            incident['typeInfo']['whereIsTheGraffitiLocated'] = request.form['gloc']

        elif str(request.form['serviceRequestType'])== 'Pothole in Street':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['numberOfPotholesFilledOnBlock'] = int(request.form['npotholes'])

        elif str(request.form['serviceRequestType'])== 'Rodent Baiting/Rat Complaint':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['numberOfPremisesBaited'] = request.form['prebaited']
            incident['typeInfo']['numberOfPremisesWithGarbage'] = request.form['pregarbage']
            incident['typeInfo']['numberOfPremisesWithRats'] = request.form['prerats']

        elif str(request.form['serviceRequestType'])== 'Sanitation Code Violation':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['whatIsTheNatureOfThisCodeViolation'] = request.form['codeviol']

        elif str(request.form['serviceRequestType'])== 'Tree Debris':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['ifYes-WhereIsTheDebrisLocated?'] = request.form['debrisloc']

        elif str(request.form['serviceRequestType'])== 'Tree Trim':
            
            incident['typeInfo'] = {}
            incident['typeInfo']['locationOfTrees'] = request.form['trimloc']

        if 'ssa' in request.form:
            incident['ssa'] = int(request.form['ssa'])

        if 'activity' in request.form:
            incident['activity'] = {}
            incident['activity']['currentActivity'] = request.form['currentActivity']
            incident['activity']['mostRecentAction'] = request.form['mostRecentAction']
        
        if 'area' in request.form:
            incident['area'] = {}
            incident['area']['historicalWards2003-2015'] = int(request.form['hwards'])
            incident['area']['zipCodes'] = int(request.form['zipCodes'])
            incident['area']['communityAreas'] = int(request.form['communityAreas'])
            incident['area']['censusTracts'] = int(request.form['censusTracts'])
            incident['area']['wards'] = int(request.form['wards'])            

        mongo.db.request.insert_one(incident)

        return ("Incident inserted successfully.")
    except:
        return ("An exception occurred.")

@app.route('/upvote_request', methods = ['POST'])
def upvote_request():

    try:        
        citizenId = request.form['citizenId']
        requestId = request.form['requestId']

        mongo.db.request.update(
        { '_id': ObjectId(requestId)},
        {
            '$addToSet': {
            'upvotedByCitizensWithId': int(citizenId)
            }
        }
        )

        mongo.db.citizens.update(
        { '_id': citizenId},
        {
            '$addToSet': {
            'upvotedRequests': ObjectId(requestId)
            }
        }
        )

        return ("Upvote casted successfully.")
    except:
        return ("An exception occurred.")
    
@app.route('/q1', methods = ['POST'])
def find_query_1():

    try:
        startDateInput= request.form['startDate'] 
        startDate = startDateInput.split('-')
        endDateInput= request.form['endDate'] 
        endDate = endDateInput.split('-')
        
        query = mongo.db.request.aggregate([
            {'$match': {"creationDate": {'$gte': datetime.datetime(int(startDate[0]), int(startDate[1]), int(startDate[2])), '$lt': datetime.datetime(int(endDate[0]), int(endDate[1]), int(endDate[2]))}}},
            {'$group': {'_id': "$serviceRequestType", 'total': {'$sum': 1}}},
            {'$sort': {"total": -1}}
        ])

        documents = [doc for doc in query]

        return jsonify({'result' : documents})
    
    except:
        return ("Try a different time range.")
    
@app.route('/q2', methods = ['POST'])
def find_query_2():
    
    try:
        startDateInput= request.form['startDate'] 
        startDate = startDateInput.split('-')
        start = datetime.datetime(int(startDate[0]), int(startDate[1]), int(startDate[2]))

        endDateInput= request.form['endDate'] 
        endDate = endDateInput.split('-')
        end = datetime.datetime(int(endDate[0]), int(endDate[1]), int(endDate[2]))

        query =  mongo.db.request.aggregate([
            {'$match': {"serviceRequestType": str(request.form['srType']), "creationDate": {'$gte': start, '$lt': end}}},
            {'$group': {'_id': "$creationDate", 'total': {'$sum': 1}}}
        ])

        documents = [doc for doc in query]

        return jsonify({'result' : documents})
    
    except:
        return ("Try a different time range and/or service request type.")

@app.route('/q3', methods = ['POST'])
def find_query_3():

    try:
        startDateInput= request.form['startDate'] 
        startDateSplit = startDateInput.split('-')
        startDate = datetime.datetime(int(startDateSplit[0]), int(startDateSplit[1]), int(startDateSplit[2]))
        endDate = startDate + datetime.timedelta(days=1)

        query = mongo.db.request.aggregate([
            {'$match': { "creationDate": {'$gte': startDate, '$lt': endDate}}},
            {'$group': {'_id': {'zipCode': "$zipCode", 'serviceRequestType': "$serviceRequestType"}, 'total':{'$sum':1}}},
            {'$sort': {"_id.zipCode": 1, "total": -1}},  
            {'$group': {'_id': "$_id.zipCode", 'serviceRequestType': {'$push': {'sr':"$_id.serviceRequestType", 'total': "$total"}}}},
            {'$project': {'_id': 1, 'mostCommonServiceRequestTypes': {'$slice':["$serviceRequestType", 3]}}}
        ])

        documents = [doc for doc in query]

        return jsonify({'result' : documents})
    
    except:
        return ("Try a different date.")

@app.route('/q4', methods = ['POST'])
def find_query_4():

    try:
        
        srType= request.form['srType'] 

        query = mongo.db.request.aggregate([
            {'$match': {"serviceRequestType": str(srType), "ward": { '$nin': [0, 'NaN'] }}},
            {'$group': {'_id': "$ward", 'total': {'$sum': 1}}},
            {'$sort': {"total": 1}},
            {'$limit': 3}
        ])

        documents = [doc for doc in query]

        return jsonify({'result' : documents})

    except:
        return ("Try a different service request type.")

@app.route('/q5', methods = ['POST'])
def find_query_5():

    try:
        startDateInput= request.form['startDate'] 
        startDate = startDateInput.split('-')
        start = datetime.datetime(int(startDate[0]), int(startDate[1]), int(startDate[2]))

        endDateInput= request.form['endDate'] 
        endDate = endDateInput.split('-')
        end = datetime.datetime(int(endDate[0]), int(endDate[1]), int(endDate[2]))
    
        query = mongo.db.request.aggregate([
            {'$match': {"creationDate": {'$gte': start, '$lt': end}}},
            {'$project': {'serviceRequestType': 1, 'completionTime': {'$subtract': ["$completionDate", "$creationDate"]}}},
            {'$group': {'_id': "$serviceRequestType", 'averageCompletionTime': {'$avg': "$completionTime"}}},
            {'$project': {'_id':1, 'averageCompletionTimeInWorkdays': { '$divide': [ "$averageCompletionTime", 28800000] }}}
        ])

        documents = [doc for doc in query]

        return jsonify({'result' : documents})

    except:
        return ("Try different dates.")

@app.route('/q6', methods = ['POST'])
def find_query_6():

    try:

        lat = request.form['lat']
        lon = request.form['lon']
        dis = request.form['dis']

        startDateInput= request.form['startDate'] 
        startDateSplit = startDateInput.split('-')
        startDate = datetime.datetime(int(startDateSplit[0]), int(startDateSplit[1]), int(startDateSplit[2]))
        endDate = startDate + datetime.timedelta(days=1)
    
        query = mongo.db.request.aggregate([
            {
            '$geoNear': {
                'near': { 'type': "Point", 'coordinates': [ float(lat), float(lon)] },
                'distanceField': "dist.calculated",
                'maxDistance': float(dis),         
                'includeLocs': "dist.location",
                'spherical': True
            }
            },
            {'$match': { "creationDate": {'$gte': startDate, '$lt': endDate}}},
            {'$group': {'_id': "$serviceRequestType", 'total': {'$sum': 1}}},
            {'$sort': {"total": -1}},
            {'$limit': 1}
        ])

        documents = [doc for doc in query]

        return jsonify({'result' : documents})

    except:
        return ("Try different dates and/or bounding box specs.")

@app.route('/q7', methods = ['POST'])
def find_query_7():

    try:
        startDateInput= request.form['startDate'] 
        startDateSplit = startDateInput.split('-')
        startDate = datetime.datetime(int(startDateSplit[0]), int(startDateSplit[1]), int(startDateSplit[2]))
        endDate = startDate + datetime.timedelta(days=1)
    
        query = mongo.db.request.aggregate([
            {'$match': { "creationDate": {'$gte': startDate, '$lt': endDate}}},
            {'$project': {'_id': "$_id", 'upvotes': {'$size': { "$ifNull": [ "$upvotedByCitizensWithId", [] ] }}}},
            {'$sort': {"upvotes": -1}},
            {'$limit': 50}
        ])

        documents = [doc for doc in query]
        
        return jsonify({'result' : documents})
    
    except:
        return ("Try another date.")

@app.route('/q8', methods = ['POST'])
def find_query_8():
    
    query = mongo.db.citizens.aggregate([
        {'$project': {'_id': "$_id", 'name': "$name", 'upvotes': {'$size': { "$ifNull": [ "$upvotedRequests", [] ] }}}},
        {'$sort': {"upvotes": -1}},
        {'$limit': 50}
    ]) 

    documents = [doc for doc in query]

    return jsonify({'result' : documents})

@app.route('/q9', methods = ['POST'])
def find_query_9():

    query = mongo.db.citizens.aggregate([
    {
        '$lookup':
        {
            'from': "request",
            'localField': "upvotedRequests",
            'foreignField': "_id",
            'as': "requests"
        }
    },
    {'$project': {'_id': "$_id", 'name': "$name", 'wards': "$requests.ward"}},
    {'$unwind': "$wards"},
    {'$group': {'_id': {'_id': "$_id",'name': "$name" }, 'uniqueValuesOfWards': {'$addToSet': "$wards"}}},
    {'$project': {'_id': "$_id", 'uniqueWards': {'$size': "$uniqueValuesOfWards"}}},
    {'$sort': {"uniqueWards": -1}},
    {'$limit': 50}  
    ])

    documents = [doc for doc in query]

    return jsonify({'result' : documents})

@app.route('/q10', methods = ['POST'])
def find_query_10():

    query = mongo.db.citizens.aggregate([
        {'$group': {'_id': "$phoneNumber", 'total': {'$sum': 1}, 'data': {'$push': {'citizenId': "$_id", 'requestsIds': "$upvotedRequests"}}}},
        {'$match': {"total": { '$nin': [1] }}},
        {'$project': {'_id': "$data.requestsIds"}},
        {'$unwind': "$_id"},
        {'$unwind': "$_id"}
    ])

    documents = [doc for doc in query]

    return jsonify({'result' : documents})

@app.route('/q11', methods = ['POST'])
def find_query_11():

    try:
    
        name= request.form['name'] 
        
        query = mongo.db.citizens.aggregate([
            {'$match': { "name": name}},
            {
                '$lookup':
                {
                    'from': "request",
                    'localField': "upvotedRequests",
                    'foreignField': "_id",
                    'as': "requests"
                }
            },
            {'$project': {'_id': "$_id", 'wards': "$requests.ward"}},
            {'$unwind': "$wards"},
            {'$group': {'_id': "$wards", 'total': {'$sum': 1}}}
        ])

        documents = [doc for doc in query]

        return jsonify({'result' : documents})

    except:
        return ("Try another name.")


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)