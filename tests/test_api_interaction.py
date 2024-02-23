import unittest
from unittest.mock import patch, Mock

from src.api_interaction import post_predict_ats_ep, post_predict_resp_ep, combine_predictions, post_offer_ep, summarize
from src.member_features import MemberFeatures
from src.prediction_ep import Prediction
from src.data_processing import read_member_data, create_member_features

class TestApiInteractionFunctions(unittest.TestCase):

    @patch('requests.post')
    def test_post_predict_ats_ep(self, mock_post):
        member_id = 1
        member_features = MemberFeatures(
            AVG_POINTS_BOUGHT = 150,
            AVG_REVENUE_USD = 15,
            LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT = 150,
            LAST_3_TRANSACTIONS_AVG_REVENUE_USD = 15,
            PCT_BUY_TRANSACTIONS = 0.5,
            PCT_GIFT_TRANSACTIONS = 0.5,
            PCT_REDEEM_TRANSACTIONS = 0,
            DAYS_SINCE_LAST_TRANSACTION = 90
        )  # Sample member features
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'prediction': 150}

        prediction_ats, latency = post_predict_ats_ep(member_id, member_features)

        self.assertEqual(prediction_ats, 150)
        self.assertGreaterEqual(latency, 0.0)

    @patch('requests.post')
    def test_post_predict_resp_ep(self, mock_post):
        member_id = 1
        member_features = MemberFeatures(
            AVG_POINTS_BOUGHT = 150,
            AVG_REVENUE_USD = 15,
            LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT = 150,
            LAST_3_TRANSACTIONS_AVG_REVENUE_USD = 15,
            PCT_BUY_TRANSACTIONS = 0.5,
            PCT_GIFT_TRANSACTIONS = 0.5,
            PCT_REDEEM_TRANSACTIONS = 0,
            DAYS_SINCE_LAST_TRANSACTION = 90
        )  # Sample member features
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'prediction': 0.58}

        prediction, latency = post_predict_resp_ep(member_id, member_features)

        self.assertEqual(prediction, 0.58)
        self.assertGreater(latency, 0.0)

    def test_combine_predictions(self):
        predict_ats_ep = 150
        predict_resp_ep = 0.58

        combined_prediction = combine_predictions(predict_ats_ep, predict_resp_ep)

        self.assertEqual(combined_prediction.ats_prediction, predict_ats_ep)
        self.assertEqual(combined_prediction.resp_prediction, predict_resp_ep)

    @patch('requests.post')
    def test_post_offer_ep(self, mock_post):
        prediction = Prediction(ats_prediction=150, resp_prediction=0.58)
        member_id = 1
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {'offer': 'OFFER_1'}

        offer, latency = post_offer_ep(member_id, prediction)

        self.assertEqual(offer, 'OFFER_1')
        self.assertGreater(latency, 0.0)

    
    @patch('src.data_processing.read_member_data')
    @patch('src.data_processing.create_member_features')
    @patch('src.api_interaction.post_predict_ats_ep')
    @patch('src.api_interaction.post_predict_resp_ep')
    @patch('src.api_interaction.post_offer_ep')
    def test_summarize(self, mock_post_offer_ep, mock_post_predict_resp_ep, mock_post_predict_ats_ep, mock_create_member_features, mock_read_member_data):
        # Mock the external dependencies
        mock_read_member_data.return_value = (Mock(), 0.1)  # Mocking read_member_data
        mock_create_member_features.return_value = (MemberFeatures(), Mock())  # Mocking create_member_features
        mock_post_predict_ats_ep.return_value = (150, 0.3)  # Mocking post_predict_ats_ep
        mock_post_predict_resp_ep.return_value = (0.58, 0.4)  # Mocking post_predict_resp_ep
        mock_post_offer_ep.return_value = ("OFFER_1", 0.5)  # Mocking post_offer_ep

        member_id = 1
        dataset_file_path = 'test_members.csv'

        result = summarize(member_id, dataset_file_path)

        self.assertEqual(result['member_id'], member_id)
        self.assertIsInstance(result['member_features'], MemberFeatures)
        self.assertEqual(result['predict_ats_ep'], 150)
        self.assertEqual(result['predict_resp_ep'], 0.58)
        self.assertEqual(result['offer_ep'], 'OFFER_1')
        self.assertGreaterEqual(result['latencies']['read_data_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['transform_features_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['avg_points_bought_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['avg_revenue_usd_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['last_3_transactions_avg_points_bought_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['last_3_transactions_avg_revenue_usd_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['pct_buy_trancactions_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['pct_gift_transactions_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['pct_redeem_transactions_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['member_features_latency']['days_since_last_transaction_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['prediction_ats_ep_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['prediction_resp_ep_latency'], 0.0)
        self.assertGreaterEqual(result['latencies']['offer_ep_latency'], 0.0)