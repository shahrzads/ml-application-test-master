import pandas as pd
import datetime
from datetime import datetime
import time

import os
import logging

from .member_features import MemberFeatures

def read_member_data(file_path):
    """
    Read member data from csv file

    Parameters:
    - file_path (str): path to the csv file

    Returns:
    - pd.DataFrame: DataFrame containing member data
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    member_data = pd.read_csv(file_path)

    member_data['lastTransactionPointsBought'] = member_data['lastTransactionPointsBought'].fillna(0)
    member_data['lastTransactionRevenueUSD'] = member_data['lastTransactionRevenueUSD'].fillna(0)
    member_data['lastTransactionType'] = member_data['lastTransactionType'].fillna('')
    member_data['lastTransatcionUtcTs'] = member_data['lastTransatcionUtcTs'].fillna('1990-01-01 00:00:00')

    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Read member data from {file_path}. Latency: {latency} seconds')

    return member_data, latency


''' Transform the input member_data dataset into features for each member '''
def calculate_avg_points_bought(member_data, member_id):
    """
    Transform input data into AVG_POINTS_BOUGHT feature, calculated as $\text{total points bought} / \text{number of transactions}$ for a specific member

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the the member data
    - member_id (str): member_id for which to calculate the average points bought

    Returns:
    - float: average points bought for the specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]

    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # calculate average points bought
    avg_points_bought = member_transactions['lastTransactionPointsBought'].mean()
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated AVG_POINTS_BOUGHT for member_id {member_id}. Latency: {latency} seconds')

    return round(avg_points_bought, 2), latency

def calculate_avg_revenue_usd(member_data, member_id):
    """
    Transform input data into AVG_REVENUE_USD feature, calculated as $\text{total transaction revenue} / \text{number of transactions}$ for a specific member

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the the member data
    - member_id (str): member_id for which to calculate the average points bought

    Returns:
    - float: average revenue USD for the specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]

    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # calculate average revenue USD
    avg_revenue_usd = member_transactions['lastTransactionRevenueUSD'].mean()
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated AVG_REVENUE_USD for member_id {member_id}. Latency: {latency} seconds')

    return round(avg_revenue_usd, 2), latency

def calculate_last_3_transactions_avg_points_bought(member_data, member_id, n = 3):
    """
    Transform input data into LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT feature, which is the AVG_POINTS_BOUGHT for the 3 most recent transactions for a specific member

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the the member data
    - member_id (str): member_id for which to calculate the average points bought
    - n (int): number of recent trancations to consider

    Returns:
    - float: average points bought for the last n transactions for the specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]


    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # #### convert ot datetime format
    # last_transaction_datetime_format = datetime.strptime(member_transactions['lastTransactionUtcTs'], "%Y-%m-%d %H:%M:%S")

    # sort transactions by lastTransatcionUtcTs (date and time) in descending order
    member_transactions_sort = member_transactions.sort_values(by='lastTransatcionUtcTs', ascending=False)
    # print('sorted dataframe: ', member_transactions_sort)

    # take the first n transactions
    last_n_transactions = member_transactions_sort.head(n)

    # calculate average points bought for the last n transactions
    avg_points_bought_last_n_transactions = last_n_transactions['lastTransactionPointsBought'].mean()
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated AVG_POINTS_BOUGHT_LAST_N_TRANSACTIONS for member_id {member_id}. Latency: {latency} seconds')

    return round(avg_points_bought_last_n_transactions, 2), latency

def calculate_last_3_transactions_avg_revenue_usd(member_data, member_id, n = 3):
    """
    Transform input data into LAST_3_TRANSACTIONS_AVG_REVENUE_USD feature, which is the AVG_REVENUE_USD for the 3 most recent transactions for a specific member

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the the member data
    - member_id (str): member_id for which to calculate the average revenue USD
    - n (int): number of recent trancations to consider

    Returns:
    - float: average revenue USD for the last n transactions for the specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]

    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # sort transactions by lastTransatcionUtcTs (date and time) in descending order
    member_transactions_sort = member_transactions.sort_values(by='lastTransatcionUtcTs', ascending=False)

    # take the first n transactions
    last_n_transactions = member_transactions_sort.head(n)

    # calculate average revenue USD for the last n transactions
    avg_revenue_usd_last_n_transactions = last_n_transactions['lastTransactionRevenueUSD'].mean()
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated AVG_REVENUE_USD_LAST_N_TRANSACTIONS for member_id {member_id}. Latency: {latency} seconds')

    return round(avg_revenue_usd_last_n_transactions, 2), latency

def calculate_pct_buy_transactions(member_data, member_id):
    """
    Transform input data into PCT_BUY_TRANSACTIONS feature, as the rate (i.e., percentage) of BUY transactions for a specific member, calculated as $\text{Number of transactions where transaction type was BUY} / \text{number of transactions}$ for a specified member

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the the member data
    - member_id (str): member_id for which to calculate the percentage of BUY transaction

    Returns:
    - float: rate (i.e., percentage) of BUY transactions for a specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]

    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # calculate total number of transactions
    total_transactions = len(member_transactions)

    # calculate number of BUY transactions
    buy_transactions = len(member_transactions[member_transactions['lastTransactionType'] == 'buy'])

    # calculate the rate (i.e., percentage) of BUY transactions
    pct_buy_transactions = buy_transactions / total_transactions if total_transactions > 0 else 0.0
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated PCT_BUY_TRANSACTIONS for member_id {member_id}. Latency: {latency} seconds')

    return round(pct_buy_transactions, 2), latency

def calculate_pct_gift_transactions(member_data, member_id):
    """
    Transform input data into PCT_GIFT_TRANSACTIONS feature, as the rate (i.e., percentage) of GIFT transactions for a specific member, calculated as $\text{Number of transactions where transaction type was GIFT} / \text{number of transactions}$ for a specified member

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the the member data
    - member_id (str): member_id for which to calculate the percentage of GIFT transaction

    Returns:
    - float: rate (i.e., percentage) of GIFT transactions for a specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]

    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # calculate total number of transactions
    total_transactions = len(member_transactions)

    # calculate number of GIFT transactions
    gift_transactions = len(member_transactions[member_transactions['lastTransactionType'] == 'gift'])

    # calculate the rate (i.e., percentage) of GIFT transactions
    pct_gift_transactions = gift_transactions / total_transactions if total_transactions > 0 else 0.0
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated PCT_GIFT_TRANSACTIONS for member_id {member_id}. Latency: {latency} seconds')

    return round(pct_gift_transactions, 2), latency

def calculate_pct_redeem_transactions(member_data, member_id):
    """
    Transform input data into PCT_REDEEM_TRANSACTIONS feature, as the rate (i.e., percentage) of REDEEM transactions for a specific member, calculated as $\text{Number of transactions where transaction type was REDEEM} / \text{number of transactions}$ for a specified member

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the the member data
    - member_id (str): member_id for which to calculate the percentage of REDEEM transaction

    Returns:
    - float: rate (i.e., percentage) of REDEEM transactions for a specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]

    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # calculate total number of transactions
    total_transactions = len(member_transactions)

    # calculate number of redeem transactions
    redeem_transactions = len(member_transactions[member_transactions['lastTransactionType'] == 'redeem'])

    # calculate the rate (i.e., percentage) of redeem transactions
    pct_redeem_transactions = redeem_transactions / total_transactions if total_transactions > 0 else 0.0
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated PCT_REDEEM_TRANSACTIONS for member_id {member_id}. Latency: {latency} seconds')

    return round(pct_redeem_transactions, 2), latency

def calcualte_days_sicne_last_transaction(member_data, member_id):
    """
    calculate the number of days since the last transaction for a specific member ((ie. Current day in UTC - last day of transaction in UTC))

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the member data
    - member_id (str): member_id for which to calculate the days since last transaction

    Returns:
    - int: number of days since the last transaction for the specified member
    - latency (float): time taken to process the function
    """
    start_time = time.time()
    # filter data for the specific member id
    member_transactions = member_data[member_data['memberId'] == member_id]

    if len(member_transactions) == 0:
        logging.warning(f'No transactions for member_id {member_id}. Returning None.')
        return None, 0 # no transactions for the specified member

    # sort transactions by lastTransatcionUtcTs (date and time) in descending order
    member_transactions_sort = member_transactions.sort_values(by='lastTransatcionUtcTs', ascending=False)

    # take the UtsTs of the latest transaction
    last_transaction_time = member_transactions_sort['lastTransatcionUtcTs'].iloc[0]

    #### convert ot datetime format
    last_transaction_datetime_format = datetime.strptime(last_transaction_time, "%Y-%m-%d %H:%M:%S")

    # current time
    current_datetime_format = datetime.utcnow()

    # calculate the number of days since the last transaction
    delta_days = (current_datetime_format - last_transaction_datetime_format).days
    end_time = time.time()
    latency = end_time - start_time

    logging.info(f'Calculated DAYS_SINCE_LAST_TRANSACTION for member_id {member_id}. Latency: {latency} seconds')

    return delta_days, latency

# create member_features object based on the above functions that transform the raw features to the desired features
def create_member_features(member_data, member_id):
    """
    Combine all the transforming functions to create a MemberFeatures object

    Parameters:
    - member_data (pd.DataFrame): input DataFrame including the member data
    - member_id (str): member_id for which to calculate different transformed features

    Returns:
    - member_features (MemberFeatures): an object of MemberFeatures including the transformed member data
    - member_features_latency (dict): a dictionary containing all the latencies for different transforming functions
    """
    start_time = time.time()

    logging.debug(f'Creating member features for member_id {member_id}.')

    # compute individual features
    avg_points_bought, avg_points_bought_latency = calculate_avg_points_bought(member_data, member_id)
    avg_revenue_usd, avg_revenue_usd_latency = calculate_avg_revenue_usd(member_data, member_id)
    last_3_transactions_avg_points_bought, last_3_transactions_avg_points_bought_latency = calculate_last_3_transactions_avg_points_bought(member_data, member_id)
    last_3_transactions_avg_revenue_usd, last_3_transactions_avg_revenue_usd_latency = calculate_last_3_transactions_avg_revenue_usd(member_data, member_id)
    pct_buy_transactions, pct_buy_transactions_latency = calculate_pct_buy_transactions(member_data, member_id)
    pct_gift_transactions, pct_gift_transactions_latency = calculate_pct_gift_transactions(member_data, member_id)
    pct_redeem_transactions, pct_redeem_transactions_latency = calculate_pct_redeem_transactions(member_data, member_id)
    days_sicne_last_transaction, days_sicne_last_transaction_latency = calcualte_days_sicne_last_transaction(member_data, member_id)

    # create MemberFeatures object
    member_features = MemberFeatures(
        AVG_POINTS_BOUGHT = avg_points_bought,
        AVG_REVENUE_USD = avg_revenue_usd,
        LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT = last_3_transactions_avg_points_bought,
        LAST_3_TRANSACTIONS_AVG_REVENUE_USD = last_3_transactions_avg_revenue_usd,
        PCT_BUY_TRANSACTIONS = pct_buy_transactions,
        PCT_GIFT_TRANSACTIONS = pct_gift_transactions,
        PCT_REDEEM_TRANSACTIONS = pct_redeem_transactions,
        DAYS_SINCE_LAST_TRANSACTION = days_sicne_last_transaction
    )

    logging.debug(f'Finished creating member features for member_id {member_id}.')

    end_time = time.time()
    latency = end_time - start_time

    memebr_features_latency = {
        "transform_features_latency": latency,
        "avg_points_bought_latency": avg_points_bought_latency,
        "avg_revenue_usd_latency": avg_revenue_usd_latency,
        "last_3_transactions_avg_points_bought_latency": last_3_transactions_avg_points_bought_latency,
        "last_3_transactions_avg_revenue_usd_latency": last_3_transactions_avg_revenue_usd_latency,
        "pct_buy_trancactions_latency": pct_buy_transactions_latency,
        "pct_gift_transactions_latency": pct_gift_transactions_latency,
        "pct_redeem_transactions_latency": pct_redeem_transactions_latency,
        "days_since_last_transaction_latency": days_sicne_last_transaction_latency
    }

    return member_features, memebr_features_latency


if __name__ == "__main__":
    # get current directory
    path = os.getcwd()
    # print('current directory: ', path)
    # get parent directory
    # parent = os.path.dirname(path)
    # print('parent directory: ', parent)


    # reading dataset
    member_data, read_data_latency = read_member_data(path + '/member_data.csv')
    # print('member_data dataset head: ', member_data.head())

    avg_points_bought, avg_points_bought_latency = calculate_avg_points_bought(member_data, 'FB608F11')
    print('avg points bought: ', avg_points_bought)

    avg_revenue_usd, avg_revenue_usd_latency = calculate_avg_revenue_usd(member_data, '5D72524D')
    print('avg revenue USD: ', avg_revenue_usd)

    last_3_transactions_avg_points_bought, last_3_transactions_avg_points_bought_latency = calculate_last_3_transactions_avg_points_bought(member_data, '5D72524D')
    print('last 3 transactions avg points bought: ', last_3_transactions_avg_points_bought)

    last_3_transactions_avg_revenue_usd, last_3_transactions_avg_revenue_usd_latency = calculate_last_3_transactions_avg_revenue_usd(member_data, '5D72524D')
    print('last 3 transactions avg revenue USD: ', last_3_transactions_avg_revenue_usd)

    pct_buy_transactions, pct_buy_transactions_latency = calculate_pct_buy_transactions(member_data, '5D72524D')
    print('pct buy transactions: ', pct_buy_transactions)

    pct_gift_transactions, pct_gift_transactions_latency = calculate_pct_gift_transactions(member_data, '5D72524D')
    print('pct gift transactions: ', pct_gift_transactions)

    pct_redeem_transactions, pct_redeem_transactions_latency = calculate_pct_redeem_transactions(member_data, '5D72524D')
    print('pct redeem transactions: ', pct_redeem_transactions)

    days_sicne_last_transaction, days_sicne_last_transaction_latency = calcualte_days_sicne_last_transaction(member_data, '5D72524D')
    print('days since last transaction: ', days_sicne_last_transaction)