
import unittest
import csv
import time
from market.order_book import OrderBook, FormattedMessage


class TestOrderBook(unittest.TestCase):
    def test_aapl_run(self):
        # use AAPL data for testing
        filename = "data/AAPL-20170102-v2.csv"
        print("Checking %s:" % filename)
        with open(filename, "r") as f:
            reader = csv.reader(f)
            order_book = OrderBook()
            start = time.clock()
            counter = 0
            for row in reader:
                counter += 1
                order_book.process_message(FormattedMessage(row))
        self.assertLessEqual(time.clock() - start, 14)
        self.assertEqual(counter, 1733483)
        self.assertEqual(len(order_book.ask_book.pool), 0)
        self.assertEqual(len(order_book.ask_book.level_pool), 0)
        self.assertEqual(len(order_book.bid_book.pool), 0)
        self.assertEqual(len(order_book.bid_book.level_pool), 0)
        self.assertEqual(len(order_book.ask_book.volumes), 1876)
        self.assertEqual(len(order_book.bid_book.volumes), 3175)


if __name__ == "__main__":
    unittest.main()