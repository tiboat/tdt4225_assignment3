from DbConnector import DbConnector
import os
import pandas as pd

# TODO:
# Is amount of activities different than previous dataset?
# Are date times, longitude, ... ok to insert as string?


def read_labeled_users():
    """
    Reads the labeled_ids.txt file and returns the list of users mentioned in this file
    (which means these have labeled data)

    Returns: list of users (as strings) which have labeled data
    """
    label_file_path = "./dataset/dataset/labeled_ids.txt"
    # Reading files based on: https://stackoverflow.com/questions/3277503/how-to-read-a-file-line-by-line-into-a-list
    with open(label_file_path) as label_file:
        lines = label_file.readlines()
        return [line.rstrip() for line in lines]


def get_start_and_end_time(trackpoints):
    """
    Returns the start and end date time of a dataframe of an activity given its dataframe of trackpoints.
    The start time is the date and time of the first trackpoint and end time that of the last trackpoint.

    Args:
        trackpoints: dataframe of trackpoints of an activity

    Returns: start and end date time

    """
    start_time = trackpoints["date"].iloc[0] + " " + trackpoints["time"].iloc[0]
    end_time = trackpoints["date"].iloc[-1] + " " + trackpoints["time"].iloc[-1]

    return start_time, end_time


class Setup:

    def __init__(self):
        self.root_data_dir = "./dataset/dataset/Data/"
        self.labeled_users = read_labeled_users()
        self.labels = {}
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_collections(self):
        print("Creating collections...")
        self.db.create_collection("User")
        self.db.create_collection("Activity")
        self.db.create_collection("TrackPoint")

    def insert_data_in_collections(self):
        """
        Inserts the user, activity and trackpoint data in the database
        """
        users = []
        activities = []
        trackpoints = []
        activity_id = 0
        trackpoint_id = 0
        for user in os.listdir(self.root_data_dir):
            print(f"Handling data from user {user} ...")
            activities_user = []
            activity_dir = os.path.join(self.root_data_dir, user + "/Trajectory/")
            for activity in os.listdir(activity_dir):
                trackpoints_activity = pd.read_csv(os.path.join(activity_dir, activity),
                                                   names=["lat", "lon", "not_used", "altitude", "date_days", "date",
                                                          "time"],
                                                   skiprows=6).drop(columns=["not_used"])
                # Only insert activity if it has <= 2500 trackpoints
                nr_of_trackpoints = trackpoints_activity.shape[0]
                if nr_of_trackpoints <= 2500:
                    #  -- Append trackpoint data --
                    # Add _id and activity id to dataframe
                    trackpoint_activity_ids = list(range(trackpoint_id, trackpoint_id+nr_of_trackpoints))
                    trackpoints_activity['_id'] = trackpoint_activity_ids
                    trackpoints_activity['activity_id'] = activity_id
                    # Convert dataframe to list
                    trackpoints_activity_list = list(trackpoints_activity.to_dict(orient='index').values())

                    trackpoints += trackpoints_activity_list
                    trackpoint_id += nr_of_trackpoints

                    #  -- Append activity data --
                    # Get activity attributes
                    start_date_time, end_date_time = get_start_and_end_time(trackpoints_activity)
                    transportation_mode = self.get_transportation_mode(start_date_time, end_date_time, user)
                    # Add activity to the list (with reference to user and trackpoint ids list)
                    activities.append({"_id": activity_id, "user_id": user, "transportation_mode": transportation_mode,
                                       "start_date_time": start_date_time, "end_date_time": end_date_time,
                                       "trackpoints": trackpoint_activity_ids})

                    activities_user.append(activity_id)
                    activity_id += 1

            # Add user with has_label and ids of its activities
            users.append({"_id": user, "has_labels": self.has_label(user), "activities": activities_user})

        # Insert data in db
        self.bulk_insert_data("User", users)
        self.bulk_insert_data("Activity", activities)
        self.bulk_insert_data("TrackPoint", trackpoints)

        print("Users:")
        print(f"length: {len(users)}")
        # print(f"example: {users[5]}")

        print("Activity:")
        print(f"length: {len(activities)}")
        # print(f"example: {activities[5]}")

        print("Trackpoint:")
        print(f"length: {len(trackpoints)}")
        # print(f"example: {trackpoints[5]}")

    def bulk_insert_data(self, collection_name, data):
        """

        Args:
            data:
        """
        print(f"Inserting data in {collection_name} ...")
        self.db[collection_name].insert_many(data)

    def has_label(self, user):
        """
        Returns whether this user has labeled data

        Args:
            user: string which represents a user

        Returns: true if the given user has labeled data; otherwise false
        """
        return user in self.labeled_users

    def get_transportation_mode(self, start_date_time, end_date_time, user):
        """
        Returns the transportation mode of the activity from user with start start_date_time and end end_date_time.

        Args:
            start_date_time: start date time of the activity
            end_date_time: end date time of the activity
            user: user id of the concerning user

        Returns: transportation mode ("NULL" if user has no transportation labels)
        """
        if self.has_label(user):
            # If the user did not get processed yet, the labels still need to be read
            if user not in self.labels.keys():
                self.add_labels_user(user)
            start_and_end_times = list(zip(self.labels[user]["start_date_time"].tolist(),
                                           self.labels[user]["end_date_time"].tolist()))

            if (start_date_time, end_date_time) in start_and_end_times:
                index = start_and_end_times.index((start_date_time, end_date_time))
                return self.labels[user]["transportation_mode"].iloc[index]

        # If user has no labels, return "NULL"
        return "NULL"

    def add_labels_user(self, user):
        """
        Reads and adds labels from user to this object.
        This function assumes that user has labels in the labels.txt of its directory.

        Args:
            user: user id of the concerning user
        """
        label_file_path = self.root_data_dir + user + "/labels.txt"
        labels_user = pd.read_csv(label_file_path, names=["start_date_time", "end_date_time", "transportation_mode"],
                                  sep="\t", header=None, skiprows=1)
        for _, row in labels_user.iterrows():
            row["start_date_time"] = row["start_date_time"].replace("/", "-")
            row["end_date_time"] = row["end_date_time"].replace("/", "-")

        self.labels[user] = labels_user


def main():
    program = None
    try:
        # 1. Connect to MySQL server on virtual machine
        program = Setup()

        # 2. Create and define the collections User, Activity and TrackPoint
        program.create_collections()

        # 3. Insert the data from the Geolife dataset into the database
        program.insert_data_in_collections()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
