import pandas as pd
import re
#  Preprocessing function
def normalize_description(desc):
    """
    :param desc: the description of the event
    :return:
    """
    # Make the string lowercase to handle irregular case scenarios
    if isinstance(desc, str):  # Check if the value is a string
        desc = desc.lower()
    # desc = re.sub(r'\bwooden support\b', 'wooden supports', desc, flags=re.IGNORECASE).strip()
    # Use regex to remove any occurrences of 'start' or 'end' with any case
    # desc = re.sub(r'\bstart\b|\bend\b', '', desc,flags=re.IGNORECASE).strip()
    return desc

#  Preprocessing function
def process_dataframe(dataframe, delete_column, delete_events):
    """
    :param dataframe: initial dataframe
    :param delete_column: column that I want to check the values
    :param delete_events: delete those values
    :return: the preprocessed data
    """
    # Check if delete_events is a list, if not, raise AssertionError
    assert isinstance(delete_events, list), "Error in process_dataframe, delete_events must be a list"
    assert isinstance(dataframe, pd.DataFrame), "Error in process_dataframe, dataframe must be a Pandas DataFrame"
    assert isinstance(delete_column, str), "Error in process_dataframe, delete_column must be a string"

    # Use DataFrame's `isin` method to check if the delete_column contains
    # any of the delete_values and use the `~`
    # operator to get the inverse of that condition
    # Then use this condition to filter the dataframe
    filtered_dataframe = dataframe[~dataframe[delete_column].isin(delete_events)]

    return filtered_dataframe


# This function calculates the duration of each task
def calculate_task_duration(dataframe, event_column, grouped_column, timestamp_column):
    # Initialize a list to store each group's data
    all_groups_data = []

    # Group the dataframe by 'recording name' column
    grouped_data = dataframe.groupby(grouped_column)
    # For each group, calculate the unique event values and their counts
    for name, group in grouped_data:
        # Get the start and end event names
        start_events = group[group[event_column].str.contains("start")]
        end_events = group[group[event_column].str.contains("end")]

        # Initialize a dictionary to store durations
        event_durations = {}
        total_time = None
        # Loop through start events
        for start_event in start_events[event_column]:
            # Get the corresponding end event name by replacing 'start' with 'end'
            end_event = start_event.replace('start', 'end')

            if start_event == "takeoff from home point start":
                start_timestamp = group[group[event_column] == start_event][timestamp_column].iloc[0]
                end_timestamp = group[group[event_column] == "landing end"][timestamp_column].iloc[0]
                # Calculate the takeoff start from homepoint minus landing end
                total_time = end_timestamp - start_timestamp

            # Check if the end event exists
            if end_event in end_events[event_column].values:
                # Get the start and end timestamps
                start_timestamp = group[group[event_column] == start_event][timestamp_column].iloc[0]
                end_timestamp = group[group[event_column] == end_event][timestamp_column].iloc[0]

                # Calculate the duration and store it in the dictionary
                duration = end_timestamp - start_timestamp
                event_name = start_event.replace(' start', '')
                event_durations[event_name] = duration  # Use the event name as the key
                event_durations[start_event.replace(' start', '')] = duration
                event_durations["Recording name"] = group["Recording name"].values[0]
                event_durations["Total time"] = total_time
        # Append the dictionary to the list as a dataframe
        all_groups_data.append(pd.DataFrame([event_durations]))

    # Concatenate all group data into one dataframe
    final_dataframe = pd.concat(all_groups_data, ignore_index=True)

    return final_dataframe


def add_empty_columns(dataframe, add_list):
    for list_name in add_list:
        dataframe[list_name] = ''


# Insert csv file
df = pd.read_csv(r"kousoulos_experiment Data export.csv")

# Get preprocess dataframe
filtered_df = process_dataframe(dataframe=df, delete_column='Event',
                                delete_events=["MouseEvent", "KeyboardEvent", "Eye tracker Calibration start",
                                               "Eye tracker Calibration end", "RecordingStart", "ScreenRecordingStart",
                                               "ScreenRecordingEnd", "RecordingEnd"])

# Keep the indices in the filtered df
filtered_df = filtered_df.copy()

# Create the normalized desc column
filtered_df['Normalized_desc'] = filtered_df['Event'].apply(normalize_description)

# Column grouped by
grouped_df = filtered_df.groupby('Recording name', group_keys=False, sort=False).apply(
    lambda x: x.sort_values('Normalized_desc'))

final_df = calculate_task_duration(grouped_df, "Normalized_desc", "Recording name", "Recording timestamp")

# Extract numerical part of the 'Recording Name' and convert to integer for sorting
final_df['RecordingNumber'] = final_df['Recording name'].str.extract(r'(\d+)').astype(int)

# Sort the dataframe by the numerical column
final_df_sorted = final_df.sort_values('RecordingNumber')

# Optionally drop the helper column if not needed
final_df_sorted = final_df_sorted.drop('RecordingNumber', axis=1)

final_df_sorted = final_df_sorted.reset_index(drop=True)

# Add empty column names
add_empty_columns(final_df_sorted,
                  ["take off task falls", "head north task falls", "distance 60m falls",
                   "height -15m falls", "pass rock task falls", "wooden support task falls",
                   "bridge task falls", "turn to bridge falls", "return to home point falls",
                   "landing falls"]
                  )
# Reindex tre columns
final_df_sorted = final_df_sorted[[
    "Recording name","takeoff from home point", "take off task falls", "head north", "head north task falls",
    "distance 60m", "distance 60m falls", "height -15 meters", "height -15m falls",
    "pass rock", "pass rock task falls", "wooden supports", "wooden support task falls",
    "bridge task", "bridge task falls", "turn to bridge", "turn to bridge falls",
    "return to homepoint", "return to home point falls", "landing", "landing falls", "Total time"
]]
# print(final_df_sorted)
final_df_sorted.to_excel('takeoff_duration1.xlsx', index=False)

#Check if the recording are in the right format
 #counts = grouped_df.groupby('Recording name').size()

#Uncomment to check if I put the right events to the new users
 #print(grouped_df["Normalized_desc"].value_counts())
#grouped_df.to_csv("Test.csv")
