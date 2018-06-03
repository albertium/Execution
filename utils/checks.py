
import csv
import time
from market.order_book import OrderBook, FormattedMessage
import numpy as np


def check_book(filename):
    print("Checking %s:" % filename)
    with open(filename, "r") as f:
        reader = csv.reader(f)
        prev_timestamp = 0
        order_book = OrderBook()
        start = time.clock()
        counter = 0
        for row in reader:
            counter += 1
            order_book.process_message(FormattedMessage(row))
            if counter % 1000 == 0:
                print("\rElapsed: %ds / %d" % (time.clock() - start, counter), end='', flush=True)
    print("\rTotal: %ds / %d\n" % (time.clock() - start, counter), end='', flush=True)
    print("%d %d" % (len(order_book.ask_book.pool), len(order_book.bid_book.pool)))

    count = 0
    book = order_book.ask_book
    # book = order_book.bid_book
    for price, level in book.level_pool.items():
        for order in level:
            count += order.valid
    print("\n%d" % count)
    print("ask volume level: ", len(book.volumes))