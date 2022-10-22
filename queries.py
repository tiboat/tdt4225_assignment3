from pprint import pprint
from DbConnector import DbConnector
from haversine import haversine
import pandas as pd

class Queries:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def get_user_activity_trackpoint(self):
        return self.db['User'], self.db['Activity'], self.db['TrackPoint']

    def query_1(self, user, activity, trackpoint):
        """
        How many users, activities and trackpoints are there in the dataset (after it is inserted into the database).
        """
        pprint(list(user.aggregate([
            {'$group': {'_id': "User_id", 'NbOfUsers': {'$count': {}}}}
        ])))

        pprint(list(activity.aggregate([
            {'$group': {'_id': "Activity_id", 'NbOfActivities': {'$count': {}}}}
        ])))

        pprint(list(trackpoint.aggregate([
            {'$group': {'_id': "TrackPoint_id", 'NbOfTrackPoints': {'$count': {}}}}
        ])))

    def query_2(self, user, activity):
        """
        Find the average number of activities per user.
        """
        NofActivities = activity.count_documents({})
        NofUsers = user.count_documents({})
        AvgActivitiesPerUser = NofActivities/NofUsers
        print('Average number of activities per user:', AvgActivitiesPerUser)

    def query_3(self, user):
        """
        Find the top 20 users with the highest number of activities.
        """
        pprint(list(user.aggregate([
            {'$unwind': '$activities'},
            {'$group': {'Total activities': {'$sum': 1}, '_id': '$_id'}},
            {'$sort': {'Total activities': -1}},
            {'$limit': 20}
        ])))

    def query_4(self, activity):
        """
        Find all users who have taken a taxi.
        """
        pprint(list(activity.aggregate([
            {'$match': {'transportation_mode': 'taxi'}},
            {'$group': {'_id': '$user_id'}},
            {'$sort': {'_id':1}}
        ])))

    def query_5(self, activity):
        """
        Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels.
        Do not count the rows where the mode is null.
        """
        # pprint(list(activity.distinct('transportation_mode')))
        # print(list(activity.find({'user_id' : 10})))
        pprint(list(activity.aggregate([
            {'$match': {'transportation_mode': {'$ne': None}}},
            {'$group': {'_id': '$transportation_mode', 'Total activities': {'$count': {}}}},
            {'$sort': {'Total activities': -1}}
        ])))

    def query_6a(self, activity):
        """
        Find the year with the most activities.
        """
        pprint(list(activity.aggregate([
            {'$project': {'year': {'$year' : '$start_date_time'}}},
            {'$group': {'_id': '$year', 'NofActivities':{'$sum':1}}},
            {'$sort':{'NofActivities':-1}},
            {'$limit':1}
        ])))

    def query_6b(self, activity, trackpoint):
        """
        Is this also the year with most recorded hours?
        """
        pprint(list(activity.aggregate([
            {"$addFields": {
                "delta_hours": {"$divide": [{"$subtract": ["$end_date_time", "$start_date_time"]}, 3600000]}}},
            {"$group": {"_id": {"$year": "$start_date_time"}, "recorded_hours": {"$sum": "$delta_hours"}}},
            {"$sort": {"recorded_hours": -1}},
            {"$limit": 1}
        ])))

    def query_7(self, user, activity, trackpoint):
        """
        Find the total distance (in km) walked in 2008, by user with id=112.
        """

        activity_ids_and_trackpoints = (list(activity.aggregate([
            {'$match': {
                'transportation_mode': 'walk',
                'user_id': 112}},
            {'$project': {
                'trackpoints': 1
            }}
        ])))


        previous_lat_lon = None
        distance = 0
        for elem in activity_ids_and_trackpoints:
            trackpoint_list = elem['trackpoints']
            for trackpoint_id in trackpoint_list:
                lat_lon = (list(trackpoint.find({'_id': trackpoint_id},
                                                {'lat': 1,
                                                 'lon': 1})))[0]
                current_lat_lon = (lat_lon['lat'],lat_lon['lon'])
                if previous_lat_lon is None:
                    previous_lat_lon = current_lat_lon
                distance += haversine(current_lat_lon,previous_lat_lon, unit="km")
                previous_lat_lon = current_lat_lon
            previous_lat_lon = None
        print(distance, 'km')

    def query_8(self, user, activity, trackpoint):
        """
        Find the top 20 users who have gained the most altitude meters.
        """
        feet_to_meter = 0.3048
        # Fetching users first, since it is faster to query a subset of the activities.
        res = []
        for j, users in enumerate(range(182)):
            user_tracks = list(user.aggregate(
                [{
                    '$match': {
                        '_id': users
                    }
                }, {
                    '$lookup': {
                        'from': 'Activity',
                        'localField': 'activities',
                        'foreignField': '_id',
                        'as': 'activities'
                    }
                }, {
                    '$unwind': '$activities'
                }, {
                    '$unwind': '$activities.trackpoint'
                }, {
                    '$match': {
                        'activities.trackpoint.altitude': {
                            '$ne': -777
                        }
                    }
                }, {
                    '$project': {
                        '_id': 0,
                        'activity_id': '$activities._id',
                        'altitude': '$activities.trackpoint.altitude',
                        'date_time': '$activities.trackpoint.date_time'
                    }
                }]))
            altitude_gained = 0
            for i in range(len(user_tracks) - 1):
                altitude_gained += max(user_tracks[i + 1]["altitude"] - user_tracks[i]["altitude"], 0)
            res.append(pd.Series([user, altitude_gained], index=["user_id", "altitude_gained"]))
        res = pd.DataFrame(res).sort_values(by=["altitude_gained"], ascending=False)
        res["altitude_gained"] = res['altitude_gained'] * feet_to_meter
        print()
        print(res[:20])

    def query_9(self, trackpoint):
        """
        Find all users who have invalid activities, and the number of invalid activities per user.
        """
        # Based on: https://github.com/simenojensen/TDT4225_Assignment_3/blob/main/strava/queries.py

        query = list(trackpoint.aggregate([
            # Make a window in which each trackpoint gets combined with the next trackpoint (based on id)
            # if they belong to the same activity
            {"$setWindowFields": {
                "partitionBy": "$activity_id",
                "sortBy": {"_id": 1}, # trackpoint id
                "output": {
                    "next_date_time": {
                        "$shift": {"output": "$date_time", "by": 1}
                    }
                },
            }},
            # Project to new collection with the time difference between the trackpoint's datetime and
            # the next trackpoint's datetime
            {"$project": {
                "_id": 1,
                "activity_id": 1,
                "time_diff": {"$dateDiff":
                    {
                        "startDate": "$date_time",
                        "endDate": "$next_date_time",
                        "unit": "minute"
                    }
                }
            }},
            # WHERE clause: select only trackpoints which have a time difference >= 5 minutes
            {"$match": {"time_diff": {"$gte": 5}}},
            # Only keep the distinct activity ids which have consecutive trackpoints with a time difference >= 5 mins
            # so these activities are invalid
            {"$group": {"_id": "$activity_id"}},
            # Join with Activity collection
            {"$lookup": {
                "from": "Activity",
                "localField": "_id",
                "foreignField": "_id",
                "as": "InvalidActivity",
            }},
            # Count amount of invalid activities per user
            {"$sortByCount": "$InvalidActivity.user_id"}
        ]))

        pprint(query)

    def query_10(self, trackpoint, activity):
        """
        Find the users who have tracked an activity in the Forbidden City of Beijing.
        """
        fc_activities=list(trackpoint.find({
            'lat': {'$gt': 39.9160, '$lt': 39.9169},
            'lon': {'$gt': 116.3970 , '$lt': 116.3979}
        }, {'activity_id': 1}).distinct('activity_id'))

        pprint(list(activity.find({'_id': {'$in': fc_activities}},{'user_id': 1}).distinct('user_id')))

    def query_11(self, user, activity):
            """
            Find all users who have registered transportation_mode and their most used transportation_mode.
            """

            user_ids = (user.aggregate([
                {'$match': {'has_labels': {'$exists': "true", "$ne": False}}},
                {'$sort': {'_id': 1}},
                {'$project': {'_id': 1}}
            ]))
            # print(user_ids)
            for elem in user_ids:
                user_id = elem['_id']
                # print(user_id)
                total_activities_and_favourite_transport = (list(activity.aggregate([
                    {'$match': {'transportation_mode': {'$ne': None, '$exists': 'true'}, 'user_id' : user_id}},
                    {'$group': {'_id': '$transportation_mode', "Total activities": {'$count': {}}}},
                    {'$sort': {'Total activities': -1}},
                    {'$limit': 1}
                ])))
                if total_activities_and_favourite_transport != []:
                    favourite_transportation_mode = total_activities_and_favourite_transport[0]['_id']
                    print(user_id, favourite_transportation_mode)




def main():
    program = None

    try:
        program = Queries()
        user, activity, trackpoint = program.get_user_activity_trackpoint()
        # print('Query 1: ')
        # program.query_1(user, activity, trackpoint)
        #print('Query 2: ')
        #program.query_2(user, activity)
        # print('Query 3: ')
        # program.query_3(user)
        #print('Query 4: ')
        #program.query_4(activity)
        # print('Query 5: ')
        # program.query_5(activity)
        #print('Query 6a: ')
        #program.query_6a(activity)
        #print('Query 6b: ')
        #program.query_6b(activity, trackpoint)
        # print("Query 7: ")
        # program.query_7(user, activity, trackpoint)
        print('Query 8: ')
        program.query_8(user, activity, trackpoint)
        # print("Query 9: ")
        # program.query_9(trackpoint)
        #print('Query 10: ')
        #program.query_10(trackpoint, activity)
        #print('Query 11: ')
        #program.query_11(user, activity)



    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
