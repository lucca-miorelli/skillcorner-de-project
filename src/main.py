#########################################################################
###                         PROCESSING SCRIPT                         ###
#########################################################################

import os
import json
import logging
import pandas as pd


#########################################################################
###                             VARIABLES                             ###
#########################################################################

MATCHES_TO_PROCESS = {
    "10000": ["10000_metadata.json", "10000_tracking.txt"],
    "10009" : ["10009_metadata.json", "10009_tracking.txt"],
    "10013" : ["10013_metadata.json", "10013_tracking.txt"],
    "10017" : ["10017_metadata.json", "10017_tracking.txt"],
    "100094" : ["100094_metadata.json", "100094_tracking.txt"],
}
PROCESSED_DATA_PATH = os.path.join("data", "processed", "{}")


#######################################
#             SETUP LOGGING           #
#######################################

# Set up logging
logging.basicConfig(
    filename='script.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


#########################################################################
###                             FUNCTIONS                             ###
#########################################################################

def read_txt(file_path):
    """
    Reads a text file containing JSON objects line by line and returns a list of JSON objects.

    Args:
        file_path (str): The path to the text file.

    Returns:
        list: A list of JSON objects read from the file.
    """
    # Create list of json objects
    json_data = []

    # Read file line by line
    with open(file_path, 'r') as file:
        for line in file:
            # Assuming each line contains a valid JSON object
            json_object = json.loads(line)
            json_data.append(json_object)

    # Convert list of json objects to json
    tracking_json = json.dumps(json_data, indent=4, ensure_ascii=False)

    return json_data


def read_json(file_path):
    """
    Reads a JSON file and returns the parsed JSON data.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: Parsed JSON data.
    """
    # Read json file
    with open(file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    return json_data


def process_trackable_object(data, match_id):
    """
    Processes trackable object data and returns a DataFrame.

    Args:
        data (list): List of dictionaries containing trackable object data.
        match_id (int): The match ID.

    Returns:
        pd.DataFrame: DataFrame containing processed trackable object data.
    """
    # Create a list
    trackable_object_list = []

    for timeframe_data in data:
        timeframe = timeframe_data['frame']

        for track_frame in timeframe_data['data']:
            track_id = track_frame['track_id']
            trackable_object = track_frame['trackable_object']
            x = track_frame['x']
            y = track_frame['y']
            z = track_frame.get('z', None)

            # Append data to the list as a dictionary
            trackable_object_list.append({
                'frame': timeframe,
                'match_id': match_id,
                'track_id': track_id,
                'trackable_object': trackable_object,
                'x': x,
                'y': y,
                'z': z
            })

    # Convert the list of dictionaries to a DataFrame
    trackable_object_table = pd.DataFrame(trackable_object_list)

    # Display the DataFrame
    print_df(trackable_object_table, "trackable_object")

    return trackable_object_table


def process_timeframe(data, match_id):
    """
    Processes timeframe data and returns a DataFrame.

    Args:
        data (list): List of dictionaries containing timeframe data.
        match_id (int): The match ID.

    Returns:
        pd.DataFrame: DataFrame containing processed timeframe data.
    """
    # Create a list
    timeframe_list = []

    for timeframe_data in data:
        timeframe = timeframe_data['frame']
        timestamp = timeframe_data['timestamp']
        period = timeframe_data['period']
        possession_group = timeframe_data['possession']['group']
        possession_trackable_object = timeframe_data['possession']['trackable_object']

        if len(timeframe_data['image_corners_projection']) == 0:
            a, b, c, d, e, f, g, h = None, None, None, None, None, None, None, None
        else:
            # Assuming image_corners_projection is a list of 8 values
            a = timeframe_data['image_corners_projection'][0]
            b = timeframe_data['image_corners_projection'][1]
            c = timeframe_data['image_corners_projection'][2]
            d = timeframe_data['image_corners_projection'][3]
            e = timeframe_data['image_corners_projection'][4]
            f = timeframe_data['image_corners_projection'][5]
            g = timeframe_data['image_corners_projection'][6]
            h = timeframe_data['image_corners_projection'][7]

        # Append data to the list as a dictionary
        timeframe_list.append({
            'frame': timeframe,
            'match_id': match_id,
            'timestamp': timestamp,
            'period': period,
            'possession_group': possession_group,
            'possession_trackable_object': possession_trackable_object,
            'a': a,
            'b': b,
            'c': c,
            'd': d,
            'e': e,
            'f': f,
            'g': g,
            'h': h
        })

    # Convert the list of dictionaries to a DataFrame
    timeframe_table = pd.DataFrame(timeframe_list)

    # Display the DataFrame
    print_df(timeframe_table, "timeframe")

    return timeframe_table


def process_metadata(data):
    """
    Processes metadata JSON data into multiple DataFrames using pandas.

    Args:
        data (dict): Metadata JSON data.
        match_id (int): The match ID.

    Returns:
        list: List of DataFrames containing processed metadata.
    """
    # Normalize JSON data into multiple dataframes using pandas
    matches_df = pd.json_normalize(data, sep='_').drop(columns="players")
    stadiums_df = pd.json_normalize(data['stadium'])
    teams_df = pd.concat([pd.json_normalize(data['home_team']), pd.json_normalize(data['away_team'])], ignore_index=True)
    coaches_df = pd.concat([pd.json_normalize(data['home_team_coach']), pd.json_normalize(data['away_team_coach'])], ignore_index=True)
    competition_df = pd.json_normalize(data['competition_edition']['competition'])

    players_df = pd.json_normalize(data, record_path='players', sep='_')
    players_df['match_id'] = data['id']

    print_df(matches_df, "matches")
    print_df(stadiums_df, "stadiums")
    print_df(teams_df, "teams")
    print_df(coaches_df, "coaches")
    print_df(competition_df, "competition")
    print_df(players_df, "players")



    return [matches_df, stadiums_df, teams_df, coaches_df, competition_df, players_df]


def print_df(df, name):
    """
    Prints the head of a DataFrame with a formatted header.

    Args:
        df (pd.DataFrame): The DataFrame to be printed.
        name (str): Name to be displayed in the header.
    """
    # Calculate number of spaces to center the name
    spaces_len = ' '*(int((39 - len(name) - 2)/2))

    # Print dataframe
    print(
    """
    #######################################
    #{}{}{}#
    #######################################
    """.format(spaces_len, name, spaces_len)
    )
    print(df.head(30))
    print('\n')


#########################################################################
###                                MAIN                               ###
#########################################################################

if __name__ == "__main__":

    logging.info("Starting script...")

    # Create empty dataframes
    trackable_object_table = pd.DataFrame()
    timeframe_table = pd.DataFrame()
    matches_table = pd.DataFrame()
    stadiums_table = pd.DataFrame()
    teams_table = pd.DataFrame()
    coaches_table = pd.DataFrame()
    competition_table = pd.DataFrame()
    players_table = pd.DataFrame()

    # Process each match
    for match_id, data_files in MATCHES_TO_PROCESS.items():

        logging.info("Processing match {} data...".format(match_id))

        # Read data files
        metadata_file, tracking_file = data_files

        # Process data
        data = read_txt(os.path.join("data", "raw", tracking_file))
        trackable_object_data = process_trackable_object(data, match_id)
        timeframe_data = process_timeframe(data, match_id)

        # Concatenate dataframes
        trackable_object_table = pd.concat(
            [trackable_object_table, trackable_object_data])
        timeframe_table = pd.concat([timeframe_table, timeframe_data])

        logging.info("Processing match {} metadata...".format(match_id))

        # Process metadata
        metadata = read_json(os.path.join("data", "metadata", metadata_file))
        metadata_dfs = process_metadata(metadata)

        # Concatenate dataframes to respective tables
        matches_table = pd.concat([matches_table, metadata_dfs[0]], ignore_index=True)
        stadiums_table = pd.concat([stadiums_table, metadata_dfs[1]], ignore_index=True)
        teams_table = pd.concat([teams_table, metadata_dfs[2]], ignore_index=True)
        coaches_table = pd.concat([coaches_table, metadata_dfs[3]], ignore_index=True)
        competition_table = pd.concat([competition_table, metadata_dfs[4]], ignore_index=True)
        players_table = pd.concat([players_table, metadata_dfs[5]], ignore_index=True)

    logging.info("Saving dataframes to parquet...")

    # Save dataframes to parquet
    trackable_object_table.to_parquet(PROCESSED_DATA_PATH.format("trackable_object.parquet"))
    timeframe_table.to_parquet(PROCESSED_DATA_PATH.format("timeframe.parquet"))
    matches_table.to_parquet(PROCESSED_DATA_PATH.format("matches.parquet"))
    stadiums_table.to_parquet(PROCESSED_DATA_PATH.format("stadiums.parquet"))
    teams_table.to_parquet(PROCESSED_DATA_PATH.format("teams.parquet"))
    coaches_table.to_parquet(PROCESSED_DATA_PATH.format("coaches.parquet"))
    competition_table.to_parquet(PROCESSED_DATA_PATH.format("competition.parquet"))
    players_table.to_parquet(PROCESSED_DATA_PATH.format("players.parquet"))

    # Log dataframes shapes and information
    logging.info("trackable_object shape: {}".format(trackable_object_table.shape))
    logging.info("timeframe shape: {}".format(timeframe_table.shape))
    logging.info("matches shape: {}".format(matches_table.shape))
    logging.info("stadiums shape: {}".format(stadiums_table.shape))
    logging.info("teams shape: {}".format(teams_table.shape))
    logging.info("coaches shape: {}".format(coaches_table.shape))
    logging.info("competition shape: {}".format(competition_table.shape))
    logging.info("players shape: {}".format(players_table.shape))

    logging.info("Script finished.")