import pandas as pd
import os
import logging

from .api_interaction import summarize
from .member_features import MemberFeatures

def flatten_dict(nested_dict, parent_key='', sep='_'): # input parameter is itself a dict (nested dict)
    """
    flatten nested dictionaries (to handle latencies)

    Parameters:
    - membenested_dictr_id (dict): the dictionary we want to flatten
    - parent_key (str): the key for the current value, if the value is nested
    - sep (str): separator of the current key and its parent, in case there is a parent for the current key

    Returns
    -  dict: flatten dictionary of the nested input dictionary (and considering the parent keys)
    """
    logging.info(f'Flattening nested dictionary')
    temp_res = {}
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            temp_res.update(flatten_dict(value, new_key, sep))
        else:
            temp_res[new_key] = value
    return temp_res # output is a dictionary

def convert_object_to_dict(obj):
    """
    convert a class instance to dictionary

    Parameters:
    - obj (MemberFeatures): the MemberFeatures object that we want to present as a dictionary

    Returns
    -  dict: dictionary representation of the MemberFeatures object
    """
    logging.info(f'Converting class instance to dictionary')
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith('_')}
    else:
        return obj

def excel_save(member_id, dataset_file):
    """
    add all the information from a member in the dataset including the raw data, the transformed data, the predictions, the offer, and all the latencies into an excel file
    (note: if a row already exists with the current member_id, replace it with the most updates info (i.e., the new one))

    Parameters:
    - member_id (str): member_id for which to calculate different transformed features
    - dataset_file (str): path to the dataset file of the members

    Returns
    save the dataframe to excel file
    """
    logging.info(f'Saving Excel file for member_id {member_id}')
    # generate the dictionary of the required features, predictions, offers, and latencies of each of them
    curr_member_res = summarize(member_id, dataset_file)
    final_dict = {}

    for key, value in curr_member_res.items():
        if isinstance(value, dict):
            logging.info(f'Flattening nested dictionary for key {key}')
            # flatten nested dictionaries (i.e., latencies dictionary) (no nested dictionary is required)
            flat_dict = flatten_dict(value)
            final_dict.update(flat_dict)
        elif isinstance(value, MemberFeatures):
            logging.info(f'Converting MemberFeatures instance to dictionary for key {key}')
            # convert MemberFeatures class instance to dictionary
            member_features_dict = convert_object_to_dict(value)
            final_dict.update(member_features_dict)
        else:
            final_dict[key] = value

    # convert dictionary to pandas dataframe
    new_df = pd.DataFrame(data = final_dict, index = [0])
    # write pandas dataframe to excel file 
    # it is test_member_process to do the unit test
    # if not testing, user can enter the path
    xlsx_path = './test_member_process.xlsx'
    if os.path.exists(xlsx_path):
        # if the file exists, read it into a dataframe
        existing_df = pd.read_excel(xlsx_path, index_col= [0])

        # extract the meber_id from the new dataframe
        new_member_id = new_df['member_id'][0]

        # check if the member_id already exists in the existing dataframe
        if new_member_id is not None and (existing_df['member_id'].isin([new_member_id])).any():
            # remove the existing row with the same member_id (to save the most current result)
            existing_df = existing_df[existing_df['member_id'] != new_member_id]

        # append the current dataframe to the exsting one
        new_df = pd.concat([existing_df, new_df], ignore_index=True)
        
    print(new_df)

    logging.info(f'Saving DataFrame to Excel file at path {xlsx_path}')
    new_df.to_excel(xlsx_path)



if __name__ == "__main__":
    # get current directory
    path = os.getcwd()
    # get parent directory
    parent = os.path.dirname(path)

    file_name = '/member_data.csv'

    # get the path to the dataset file
    file_path = path + file_name

    # specify a member_id for testing
    # test_member_id = '5D72524D'
    test_member_id = input("Please enter member_id: ")

    excel_save(test_member_id, file_path)