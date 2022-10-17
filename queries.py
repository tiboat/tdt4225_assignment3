from datetime import datetime, time
from pprint import pprint

import pandas as pd

from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine


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
        print('Query 1: ')
        program.query_1(user, activity, trackpoint)
        print('Query 3: ')
        program.query_3(user)
        print('Query 5: ')
        program.query_5(activity)
        print("Query 7: ")
        program.query_7(user, activity, trackpoint)
        print('Query 11: ')
        program.query_11(user, activity)



    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
