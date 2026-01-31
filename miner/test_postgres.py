import unittest
from unittest.mock import MagicMock, patch

from miner.config import Config
from miner.postgres import Postgres


class TestPostgres(unittest.TestCase):
    def setUp(self):
        # Create a dummy config for testing
        self.config = Config(
            dsn="host=localhost user=test password=test dbname=test",
            log_level="INFO",
            log_database=False,
        )
        self.db = Postgres(self.config)

    @patch("miner.postgres.psycopg2.connect")
    def test_get_date_success(self, mock_connect):
        """
        Test that get_date connects, executes query, and returns the result.
        """
        # Mock the connection object and its context manager
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn

        # Mock the cursor object and its context manager
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor

        # Mock the query result (fetchone returns a tuple)
        expected_date = "2024-01-01 12:00:00"
        mock_cursor.fetchone.return_value = (expected_date,)

        # Execute
        result = self.db.get_date()

        # Assert
        self.assertEqual(result, expected_date)
        mock_connect.assert_called_once_with(self.config.dsn)
        mock_cursor.execute.assert_called_once_with("SELECT now();")

    @patch("miner.postgres.psycopg2.connect")
    def test_get_date_failure(self, mock_connect):
        """
        Test that database connection errors are re-raised.
        """
        mock_connect.side_effect = Exception("Connection refused")

        with self.assertRaises(Exception) as context:
            self.db.get_date()

        self.assertIn("Connection refused", str(context.exception))
