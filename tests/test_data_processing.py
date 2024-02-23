import unittest
import pandas as pd
import numpy as np
import datetime
from datetime import datetime, timedelta
import os

from src.data_processing import read_member_data, calculate_avg_points_bought, calculate_avg_revenue_usd, calculate_last_3_transactions_avg_points_bought, calculate_last_3_transactions_avg_revenue_usd
from src.data_processing import calculate_pct_buy_transactions, calculate_pct_gift_transactions, calculate_pct_redeem_transactions, calcualte_days_sicne_last_transaction, create_member_features

class TestMemberDataFunctions(unittest.TestCase):
    def setUp(self) -> None:
        """SetUp a sample DataFrame for testing a csv file"""
        self.test_file = 'test_members.csv'
        self.sample_data = pd.DataFrame({
            'memberId': [1, 1, 2, 2, 2, 3, 3, 3, 3, 10],
            'lastTransatcionUtcTs': [
                '2023-12-10 11:24:18',
                '2020-12-22 14:40:25',
                '2022-06-13 17:16:38',
                '2020-11-08 11:37:48',
                '2022-10-13 13:19:55',
                '2021-11-07 06:20:36',
                '2019-01-25 04:00:33',
                '2022-02-04 06:26:30',
                '2020-06-27 21:48:28',
                '2020-06-27 21:48:28'
            ],
            'lastTransactionType': ['buy', 'gift', 'redeem', 'gift', 'redeem', 'buy', 'gift', 'buy', 'gift', 'buy'],
            'lastTransactionPointsBought': [100, 200, 300, 400, 500, 600, 700, 800, 900, np.nan],
            'lastTransactionRevenueUSD': [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
            })
        self.sample_data.to_csv(self.test_file, index=False)

    def tearDown(self):
        """Cleanup code to remove the test csv file after tests run"""
        os.remove(self.test_file)

    def test_read_member_data(self):
        """Test reading member data from a csv"""
        # since read_member_data involves reading from a file, this example uses a predefined file
        data, latency = read_member_data(self.test_file)
        self.assertIsInstance(data, pd.DataFrame, "Returned object should be a pandas DataFrame")
        self.assertTrue(isinstance(latency, int) or isinstance(latency, float), "Latency should be a float")
        self.assertEqual(len(data), 10, "DataFrame should contain 9 rows")
        self.assertEqual(set(data.columns), {'memberId', 'lastTransatcionUtcTs', 'lastTransactionType', 'lastTransactionPointsBought', 'lastTransactionRevenueUSD'}, "DataFrame should contain the expected columns")
        self.assertTrue(latency >= 0, "Latency should be non-negative")

    def test_calculate_avg_points_bought(self):
        """Test the average points calculation for a given member"""
        member_data, read_data_latency = read_member_data(self.test_file)
        avg_points_bought, latency = calculate_avg_points_bought(self.sample_data, 1)
        self.assertEqual(avg_points_bought, 150, "Average points calculation is incorrect")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        avg_points_bought, latency = calculate_avg_points_bought(member_data, 10) # value nan in dataset
        self.assertEqual(avg_points_bought, 0, "Average points calculation is incorrect")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # test for a non-existent member
        avg_points_none, latency = calculate_avg_points_bought(self.sample_data, 300)
        self.assertIsNone(avg_points_none, "Function should return None for a non-existent member")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_calculate_avg_revenue_usd(self):
        """Test the average revenue USD calculation for a given member"""
        avg_revenue_usd, latency = calculate_avg_revenue_usd(self.sample_data, 1)
        self.assertEqual(avg_revenue_usd, 15, "Average revenue USD calculation is incorrect")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # test for a non-existent member
        avg_revenue_usd_none, latency = calculate_avg_revenue_usd(self.sample_data, 300)
        self.assertIsNone(avg_revenue_usd_none, "Function should return None for a non-existent member")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_calculate_last_3_transactions_avg_points_bought(self):
        member_data, read_data_latency = read_member_data(self.test_file)
        # member with more than 3 transactions, only the last 3 should be considered
        last_3_avg_points, latency = calculate_last_3_transactions_avg_points_bought(self.sample_data, 3)
        self.assertEqual(last_3_avg_points, 766.67)  # average of 600, 800, 900
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # member with exactly 3 transactions
        last_3_avg_points, latency = calculate_last_3_transactions_avg_points_bought(self.sample_data, 2)
        self.assertEqual(last_3_avg_points, 400)  # average of 300, 400, 500
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # member with nan transactions
        last_3_avg_points, latency = calculate_last_3_transactions_avg_points_bought(member_data, 10)
        self.assertEqual(last_3_avg_points, 0)  # average of nan
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # member with less than 3 transactions
        last_3_avg_points, latency = calculate_last_3_transactions_avg_points_bought(self.sample_data, 1)
        self.assertEqual(last_3_avg_points, 150)  # average of 100, 200
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # member with no transactions
        last_3_avg_points, latency = calculate_last_3_transactions_avg_points_bought(self.sample_data, 99)
        self.assertIsNone(last_3_avg_points)
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_calculate_last_3_transactions_avg_revenue_usd(self):
        # member with more than 3 transactions, only the last 3 should be considered
        last_3_avg_revenue_usd, latency = calculate_last_3_transactions_avg_revenue_usd(self.sample_data, 3)
        self.assertEqual(last_3_avg_revenue_usd, 76.67)  # average of 60, 80, 90
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # member with exactly 3 transactions
        last_3_avg_revenue_usd, latency = calculate_last_3_transactions_avg_revenue_usd(self.sample_data, 2)
        self.assertEqual(last_3_avg_revenue_usd, 40)  # average of 30, 40, 50
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # member with less than 3 transactions
        last_3_avg_revenue_usd, latency = calculate_last_3_transactions_avg_revenue_usd(self.sample_data, 1)
        self.assertEqual(last_3_avg_revenue_usd, 15)  # average of 10, 20
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # member with no transactions
        last_3_avg_revenue_usd, latency = calculate_last_3_transactions_avg_revenue_usd(self.sample_data, 99)
        self.assertIsNone(last_3_avg_revenue_usd)
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_calculate_pct_buy_transactions(self):
        pct_buy, latency = calculate_pct_buy_transactions(self.sample_data, 1)
        self.assertEqual(pct_buy, 0.5, "pct buy transaction is incorrect")
        self.assertGreaterEqual(pct_buy, 0.0) # ensure the pct buy is a non-negative value
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        pct_buy, latency = calculate_pct_buy_transactions(self.sample_data, 2) # no buy transaction for this member
        self.assertEqual(pct_buy, 0, "pct buy transaction is incorrect")
        self.assertGreaterEqual(pct_buy, 0.0) # ensure the pct buy is a non-negative value
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # test for a non-existent member
        pct_buy_none, latency = calculate_pct_buy_transactions(self.sample_data, 300)
        self.assertIsNone(pct_buy_none, "Function should return None for a non-existent member")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_calculate_pct_gift_transactions(self):
        pct_gift, latency = calculate_pct_gift_transactions(self.sample_data, 2)
        self.assertEqual(pct_gift, 0.33, "pct gift transaction is incorrect")
        self.assertGreaterEqual(pct_gift, 0.0) # ensure the pct gift is a non-negative value
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # test for a non-existent member
        pct_gift_none, latency = calculate_pct_gift_transactions(self.sample_data, 300)
        self.assertIsNone(pct_gift_none, "Function should return None for a non-existent member")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_calculate_pct_redeem_transactions(self):
        pct_redeem, latency = calculate_pct_redeem_transactions(self.sample_data, 2)
        self.assertEqual(pct_redeem, 0.67, "pct gift transaction is incorrect")
        self.assertGreaterEqual(pct_redeem, 0.0) # ensure the pct redeem is a non-negative value
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        pct_redeem, latency = calculate_pct_redeem_transactions(self.sample_data, 1) # no redeem transaction for this member
        self.assertEqual(pct_redeem, 0, "pct gift transaction is incorrect")
        self.assertGreaterEqual(pct_redeem, 0.0) # ensure the pct redeem is a non-negative value
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # test for a non-existent member
        pct_redeem_none, latency = calculate_pct_redeem_transactions(self.sample_data, 300)
        self.assertIsNone(pct_redeem_none, "Function should return None for a non-existent member")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_calcualte_days_sicne_last_transaction(self):
        days_since_last_transaction, latency = calcualte_days_sicne_last_transaction(self.sample_data, 1)
        # ensure the number of days is non-negative
        self.assertGreaterEqual(days_since_last_transaction, 0)
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

        # test for a non-existent member
        days_since_last_transaction_none, latency = calcualte_days_sicne_last_transaction(self.sample_data, 300)
        self.assertIsNone(days_since_last_transaction_none, "Function should return None for a non-existent member")
        self.assertTrue((isinstance(latency, int) or isinstance(latency, float)) and latency >= 0, "Latency should be a non-negative float")

    def test_create_member_features(self):
        member_features, latency_info = create_member_features(self.sample_data, 1)
        self.assertEqual(member_features.AVG_POINTS_BOUGHT, 150.0)
        self.assertEqual(member_features.AVG_REVENUE_USD, 15.0)
        self.assertEqual(member_features.LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT, 150)
        self.assertEqual(member_features.LAST_3_TRANSACTIONS_AVG_REVENUE_USD, 15)
        self.assertEqual(member_features.PCT_BUY_TRANSACTIONS, 0.5)
        self.assertEqual(member_features.PCT_GIFT_TRANSACTIONS, 0.5)
        self.assertEqual(member_features.PCT_REDEEM_TRANSACTIONS, 0)
        self.assertGreaterEqual(member_features.DAYS_SINCE_LAST_TRANSACTION, 0)

        # assert the latency info
        self.assertGreaterEqual(latency_info['transform_features_latency'], 0.0)
        self.assertGreaterEqual(latency_info['avg_points_bought_latency'], 0.0)
        self.assertGreaterEqual(latency_info['avg_revenue_usd_latency'], 0.0)
        self.assertGreaterEqual(latency_info['last_3_transactions_avg_points_bought_latency'], 0.0)
        self.assertGreaterEqual(latency_info['last_3_transactions_avg_revenue_usd_latency'], 0.0)
        self.assertGreaterEqual(latency_info['pct_buy_trancactions_latency'], 0.0)
        self.assertGreaterEqual(latency_info['pct_gift_transactions_latency'], 0.0)
        self.assertGreaterEqual(latency_info['pct_redeem_transactions_latency'], 0.0)
        self.assertGreaterEqual(latency_info['days_since_last_transaction_latency'], 0.0)


if __name__ == "__main__":
    unittest.main()