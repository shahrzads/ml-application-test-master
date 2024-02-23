import pandas as pd
import os
import requests
import time
import logging

from .data_processing import read_member_data, create_member_features
from .member_features import MemberFeatures
from .prediction_ep import Prediction


def post_predict_ats_ep(member_id, member_features):
    """
    POST inputs to the ATS prediction endpoint to get the estimated amount of purchase per member

    Parameters:
    - member_id (str): member_id for which to calculate the average points bought
    - member_features (MemberFeatures): an object of MemberFeatures including the transformed member data

    Returns
    - result (float or None): ATS predicted result
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # specify the url of the predict_at_ep endpoint
    predict_ats_endpoint = "http://127.0.0.1:8000/ml/ats/predict"

    logging.info(f'Sending POST request to {predict_ats_endpoint} with member_id {member_id}')

    # make a POST request to predict_ats_ep endpoint
    response = requests.post(predict_ats_endpoint, json=member_features.dict())

    if response.status_code == 200:
        result = response.json()
        logging.info(f"Prediction for member {member_id}: {result['prediction']}")
        end_time = time.time()
        latency = end_time - start_time
        return result['prediction'], latency
    else:
        logging.error(f"Error: {response.status_code} - {response.text}")
        end_time = time.time()
        latency = end_time - start_time
        return None, latency
    
def post_predict_resp_ep(member_id, member_features):
    """
    POST inputs to the RESP prediction endpoint to get the estimated likelihood of purchase per member

    Parameters:
    - member_id (str): member_id for which to calculate the average points bought
    - member_features (MemberFeatures): an object of MemberFeatures including the transformed member data

    Returns
    - result (float or None): ATS predicted result
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # specify the url of the predict_at_ep endpoint
    predict_resp_endpoint = "http://127.0.0.1:8000/ml/resp/predict"

    logging.info(f'Sending POST request to {predict_resp_endpoint} with member_id {member_id}')

    # make a POST request to predict_ats_ep endpoint
    response = requests.post(predict_resp_endpoint, json=member_features.dict())

    if response.status_code == 200:
        result = response.json()
        logging.info(f"Prediction for member {member_id}: {result['prediction']}")
        end_time = time.time()
        latency = end_time - start_time
        return result['prediction'], latency
    else:
        logging.error(f"Error: {response.status_code} - {response.text}")
        end_time = time.time()
        latency = end_time - start_time
        return None, latency

def combine_predictions(predict_ats_ep, predict_resp_ep):
    """
    Combine predictions from ATS and RESP prediction endpoints to get the object from Prediction

    Parameters:
    - predict_ats_ep (float): predicted ATS value
    - predict_resp_ep (float): predicted RESP value

    Returns
    - combined_prediction (Prediction): combined Prediction object
    """
    combined_prediction = Prediction(
        ats_prediction= predict_ats_ep,
        resp_prediction = predict_resp_ep
    )

    logging.info(f'Combining ATS and RESP predictions')

    return combined_prediction

def post_offer_ep(member_id, prediction):
    """
    POST Prediction object to the offer endpoint to get which offer should be given to the member

    Parameters:
    - member_id (str): member_id for which to calculate the average points bought
    - prediction (Prediction): an object of Prediction including the combination of ATS and RESP predictions

    Returns
    - result (str or None): the offer given to the member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # specify the url of the offer endpoint
    offer_endpoint = "http://127.0.0.1:8000/offer/assign"

    logging.info(f'Sending POST request to {offer_endpoint} with member_id {member_id}')

    # make a POST request to offer_ep endpoint
    response = requests.post(offer_endpoint, json=prediction.dict())

    if response.status_code == 200:
        result = response.json()
        logging.info(f"Offer for member {member_id}: {result['offer']}")
        end_time = time.time()
        latency = end_time - start_time
        return result['offer'], latency
    else:
        logging.error(f"Error: {response.status_code} - {response.text}")
        end_time = time.time()
        latency = end_time - start_time
        return None, latency

def summarize(member_id, dataset_file_path):
    """
    POST inputs to the ATS prediction endpoint to get the estimated amount of purchase per member,
    combine the predictions into a Prediction object,
    POST Prediction object to give offer to the member

    Parameters:
    - member_id (str): member_id for which to calculate the average points bought
    - dataset_file_path (str): path to the complete dataset

    Returns
    - result (dict): including all the predictions, combinations, offer, and latencies for each of the modules within the fucntion
    """
    logging.info(f'Summarizing data for member_id {member_id} with dataset_file_path {dataset_file_path}')
    
    # load raw dataset
    member_data, read_data_latency = read_member_data(dataset_file_path)

    # compute MemberFeatures object using the given dataset and memebr_id
    member_features, member_features_latency = create_member_features(member_data, member_id)

    # POST member features to ATS endpoint
    prediction_ats_ep_output, prediction_ats_ep_latency = post_predict_ats_ep(member_id, member_features)

    # POST member features to RESP endpoint
    prediction_resp_ep_output, prediction_resp_ep_latency = post_predict_resp_ep(member_id, member_features)

    # combine ATS and RESP predictions into the Prediction object
    combine_pred = combine_predictions(prediction_ats_ep_output, prediction_resp_ep_output)

    # predict which offer should be given to the memeber (OFFER_1 or OFFER_2)
    offer_ep_output, offer_ep_latency = post_offer_ep(member_id, combine_pred)

    res = {
        "member_id": member_id,
        "member_features": member_features,
        "predict_ats_ep": prediction_ats_ep_output,
        "predict_resp_ep": prediction_resp_ep_output,
        "offer_ep": offer_ep_output,
        "latencies": {
            "read_data_latency": read_data_latency,
            "member_features_latency": member_features_latency,
            "prediction_ats_ep_latency": prediction_ats_ep_latency,
            "prediction_resp_ep_latency": prediction_resp_ep_latency,
            "offer_ep_latency": offer_ep_latency
        }
    }

    logging.info(f'Summarization completed for member_id {member_id}')

    return res

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

    predict_output = summarize(test_member_id, file_path)
    print("predict output and latencies: ", predict_output)
    # print("latency for different functions: ", latency)