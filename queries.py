import pprint

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
        pprint.pprint(
            list(
                user.aggregate(
                    [{"$group": {"_id": "Users", "NumberOfUsers": {"$count": {}}}}]
                )
            )
        )
        pprint.pprint(
            list(
                activity.aggregate(
                    [
                        {
                            "$group": {
                                "_id": "Activities",
                                "NumberOfActivities": {"$count": {}},
                            }
                        }
                    ]
                )
            )
        )
        pprint.pprint(
            list(
                trackpoint.aggregate(
                    [
                        {
                            "$group": {
                                "_id": "Trackpoints",
                                "NumberOfTrackpoints": {"$count": {}},
                            }
                        }
                    ]
                )
            )
        )


def main():
    program = None

    try:
        program = Queries()
        user, activity, trackpoint = program.get_user_activity_trackpoint()
        program.query_1(user, activity, trackpoint)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
