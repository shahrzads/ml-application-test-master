import unittest
from unittest.mock import patch, Mock
import pandas as pd
import os

from src.excel import flatten_dict, convert_object_to_dict, excel_save
from src.api_interaction import summarize
from src.member_features import MemberFeatures

class TestExcelFunctions(unittest.TestCase):
    def test_flatten_dict(self):
        nested_dict = {"a": 1, "b": {"c": 2, "d": {"e": 3}}}
        flattened_dict = flatten_dict(nested_dict)
        expected_result = {"a": 1, "b_c": 2, "b_d_e": 3}
        self.assertEqual(flattened_dict, expected_result)

    def test_convert_object_to_dict(self):
        obj = MemberFeatures(
            AVG_POINTS_BOUGHT = 150,
            AVG_REVENUE_USD = 15,
            LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT = 150,
            LAST_3_TRANSACTIONS_AVG_REVENUE_USD = 15,
            PCT_BUY_TRANSACTIONS = 0.5,
            PCT_GIFT_TRANSACTIONS = 0.5,
            PCT_REDEEM_TRANSACTIONS = 0,
            DAYS_SINCE_LAST_TRANSACTION = 90
        )
        obj_dict = convert_object_to_dict(obj)
        expected_result = {
            "AVG_POINTS_BOUGHT" : 150,
            "AVG_REVENUE_USD" : 15,
            "LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT" : 150,
            "LAST_3_TRANSACTIONS_AVG_REVENUE_USD" : 15,
            "PCT_BUY_TRANSACTIONS" : 0.5,
            "PCT_GIFT_TRANSACTIONS" : 0.5,
            "PCT_REDEEM_TRANSACTIONS" : 0,
            "DAYS_SINCE_LAST_TRANSACTION" : 90
        }
        self.assertEqual(obj_dict, expected_result)


    @patch('src.api_interaction.summarize', return_value={
        "member_id": 1,
        "member_features": MemberFeatures(),
        "predict_ats_ep": 150,
        "predict_resp_ep": 15,
        "offer_ep": "OFFER_1",
        "latencies": {
            "read_data_latency": 0,
            "member_features_latency": {
                "transform_features_latency": 0,
                "avg_points_bought_latency": 0,
                "avg_revenue_usd_latency": 0,
                "last_3_transactions_avg_points_bought_latency": 0,
                "last_3_transactions_avg_revenue_usd_latency": 0,
                "pct_buy_trancactions_latency": 0,
                "pct_gift_transactions_latency": 0,
                "pct_redeem_transactions_latency": 0,
                "days_since_last_transaction_latency": 0
    },
            "prediction_ats_ep_latency": 0,
            "prediction_resp_ep_latency": 0,
            "offer_ep_latency": 0
        }
    })  # Mocking the summarize function
    @patch('pandas.DataFrame.to_excel')  # Mocking the to_excel method
    @patch('pandas.read_excel', return_value=pd.DataFrame({'member_id': ['5D72524D', 1], 'AVG_POINTS_BOUGHT': [760.0, 150], 'AVG_REVENUE_USD': [2.21, 15], 'LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT': [-1533.33, 150], 'LAST_3_TRANSACTIONS_AVG_REVENUE_USD': [1.37, 15], 'PCT_BUY_TRANSACTIONS': [0.4, 0.5], 'PCT_GIFT_TRANSACTIONS': [0.2, 0.5], 'PCT_REDEEM_TRANSACTIONS': [0.4, 0], 'DAYS_SINCE_LAST_TRANSACTION': [129, 0], 'predict_ats_ep': [169.07, 150], 'predict_resp_ep': [0.04, 0.58], 'offer_ep': ['OFFER_1', "OFFER_1"], 'read_data_latency': [0.02958822250366211, 0], 'member_features_latency_transform_features_latency': [0.0241241455078125, 0], 'member_features_latency_avg_points_bought_latency': [0.009762048721313477, 0], 'member_features_latency_avg_revenue_usd_latency': [0.0011909008026123047, 0], 'member_features_latency_last_3_transactions_avg_points_bought_latency': [0.004005908966064453, 0], 'member_features_latency_last_3_transactions_avg_revenue_usd_latency': [0.0013740062713623047, 0], 'member_features_latency_pct_buy_trancactions_latency': [0.0012459754943847656, 0], 'member_features_latency_pct_gift_transactions_latency': [0.0013937950134277344, 0], 'member_features_latency_pct_redeem_transactions_latency': [0.0012788772583007812, 0], 'member_features_latency_days_since_last_transaction_latency': [0.002029895782470703, 0], 'prediction_ats_ep_latency': [0.020816326141357422, 0], 'prediction_resp_ep_latency': [0.004563093185424805, 0], 'offer_ep_latency': [0.0037031173706054688, 0]}))  # Mocking read_excel
    def test_excel_save_existing_file(self, mock_read_excel, mock_to_excel, mock_summarize):
        # Call the function
        # Get the full path to the directory containing the script
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Construct the full path to the CSV file
        csv_file_path = os.path.join(script_directory, 'test_members.csv')
        excel_save(1, csv_file_path)

        # Assertions
        # mock_summarize.assert_called_once_with(1, 'test_members.csv')  # Check if summarize is called with the correct arguments

        # Extract the DataFrame from the arguments passed to to_excel
        _, args, _ = mock_to_excel.mock_calls[0]
        written_df = pd.read_excel(data = args[0])

        # Check the content of the DataFrame written to the existing file
        expected_df = pd.DataFrame({'member_id': ['5D72524D', 1], 'AVG_POINTS_BOUGHT': [760.0, 150], 'AVG_REVENUE_USD': [2.21, 15], 'LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT': [-1533.33, 150], 'LAST_3_TRANSACTIONS_AVG_REVENUE_USD': [1.37, 15], 'PCT_BUY_TRANSACTIONS': [0.4, 0.5], 'PCT_GIFT_TRANSACTIONS': [0.2, 0.5], 'PCT_REDEEM_TRANSACTIONS': [0.4, 0], 'DAYS_SINCE_LAST_TRANSACTION': [129, 0], 'predict_ats_ep': [169.07, 150], 'predict_resp_ep': [0.04, 0.58], 'offer_ep': ['OFFER_1', "OFFER_1"], 'read_data_latency': [0.02958822250366211, 0], 'member_features_latency_transform_features_latency': [0.0241241455078125, 0], 'member_features_latency_avg_points_bought_latency': [0.009762048721313477, 0], 'member_features_latency_avg_revenue_usd_latency': [0.0011909008026123047, 0], 'member_features_latency_last_3_transactions_avg_points_bought_latency': [0.004005908966064453, 0], 'member_features_latency_last_3_transactions_avg_revenue_usd_latency': [0.0013740062713623047, 0], 'member_features_latency_pct_buy_trancactions_latency': [0.0012459754943847656, 0], 'member_features_latency_pct_gift_transactions_latency': [0.0013937950134277344, 0], 'member_features_latency_pct_redeem_transactions_latency': [0.0012788772583007812, 0], 'member_features_latency_days_since_last_transaction_latency': [0.002029895782470703, 0], 'prediction_ats_ep_latency': [0.020816326141357422, 0], 'prediction_resp_ep_latency': [0.004563093185424805, 0], 'offer_ep_latency': [0.0037031173706054688, 0]})
        pd.testing.assert_frame_equal(written_df, expected_df)

    @patch('src.api_interaction.summarize', return_value={
        "member_id": 1,
        "member_features": MemberFeatures(),
        "predict_ats_ep": 150,
        "predict_resp_ep": 15,
        "offer_ep": "OFFER_1",
        "latencies": {
            "read_data_latency": 0,
            "member_features_latency": {
                "transform_features_latency": 0,
                "avg_points_bought_latency": 0,
                "avg_revenue_usd_latency": 0,
                "last_3_transactions_avg_points_bought_latency": 0,
                "last_3_transactions_avg_revenue_usd_latency": 0,
                "pct_buy_trancactions_latency": 0,
                "pct_gift_transactions_latency": 0,
                "pct_redeem_transactions_latency": 0,
                "days_since_last_transaction_latency": 0
    },
            "prediction_ats_ep_latency": 0,
            "prediction_resp_ep_latency": 0,
            "offer_ep_latency": 0
        }
    })  # Mocking the summarize function
    @patch('pandas.DataFrame.to_excel')  # Mocking the to_excel method
    @patch('pandas.read_excel', return_value=pd.DataFrame({'member_id': [1], 'AVG_POINTS_BOUGHT': [150], 'AVG_REVENUE_USD': [15], 'LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT': [150], 'LAST_3_TRANSACTIONS_AVG_REVENUE_USD': [15], 'PCT_BUY_TRANSACTIONS': [0.5], 'PCT_GIFT_TRANSACTIONS': [0.5], 'PCT_REDEEM_TRANSACTIONS': [0], 'DAYS_SINCE_LAST_TRANSACTION': [0], 'predict_ats_ep': [150], 'predict_resp_ep': [0.58], 'offer_ep': ["OFFER_1"], 'read_data_latency': [0], 'member_features_latency_transform_features_latency': [0], 'member_features_latency_avg_points_bought_latency': [0], 'member_features_latency_avg_revenue_usd_latency': [0], 'member_features_latency_last_3_transactions_avg_points_bought_latency': [0], 'member_features_latency_last_3_transactions_avg_revenue_usd_latency': [0], 'member_features_latency_pct_buy_trancactions_latency': [0], 'member_features_latency_pct_gift_transactions_latency': [0], 'member_features_latency_pct_redeem_transactions_latency': [0], 'member_features_latency_days_since_last_transaction_latency': [0], 'prediction_ats_ep_latency': [0], 'prediction_resp_ep_latency': [0], 'offer_ep_latency': [0]}))  # Mocking read_excel
    def test_excel_save_new_file(self, mock_read_excel, mock_to_excel, mock_summarize):
        # Call the function
        excel_save(1, 'test_members.csv')

        # Assertions
        # mock_summarize.assert_called_once_with(1, 'test_members.csv')  # Check if summarize is called with the correct arguments

        # Extract the DataFrame from the arguments passed to to_excel
        _, args, _ = mock_to_excel.mock_calls[0]
        written_df = pd.read_excel(data = args[0])

        # Check the content of the DataFrame written to the new file
        expected_df = pd.DataFrame({'member_id': [1], 'AVG_POINTS_BOUGHT': [150], 'AVG_REVENUE_USD': [15], 'LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT': [150], 'LAST_3_TRANSACTIONS_AVG_REVENUE_USD': [15], 'PCT_BUY_TRANSACTIONS': [0.5], 'PCT_GIFT_TRANSACTIONS': [0.5], 'PCT_REDEEM_TRANSACTIONS': [0], 'DAYS_SINCE_LAST_TRANSACTION': [0], 'predict_ats_ep': [150], 'predict_resp_ep': [0.58], 'offer_ep': ["OFFER_1"], 'read_data_latency': [0], 'member_features_latency_transform_features_latency': [0], 'member_features_latency_avg_points_bought_latency': [0], 'member_features_latency_avg_revenue_usd_latency': [0], 'member_features_latency_last_3_transactions_avg_points_bought_latency': [0], 'member_features_latency_last_3_transactions_avg_revenue_usd_latency': [0], 'member_features_latency_pct_buy_trancactions_latency': [0], 'member_features_latency_pct_gift_transactions_latency': [0], 'member_features_latency_pct_redeem_transactions_latency': [0], 'member_features_latency_days_since_last_transaction_latency': [0], 'prediction_ats_ep_latency': [0], 'prediction_resp_ep_latency': [0], 'offer_ep_latency': [0]})
        pd.testing.assert_frame_equal(written_df, expected_df)