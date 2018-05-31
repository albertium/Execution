
from market.order_book import OrderBook
from utils.parse import parse_and_save
import csv

# parse_and_save("data/S020117-v50.txt.gz", "output")

print("yes\n")

with open("data/AAPL-20170102.csv", "r") as f:
    reader = csv.reader(f)
    prev_timestamp = 0
    order_book = OrderBook()
    for row in reader:
        order_type = row[0]
        ref = int(row[2])
        timestamp = int(row[3])
        if timestamp < prev_timestamp:
            raise RuntimeError("Timestamp not ordered")

        if order_type == 'A':  # add order
            if row[4] == '1':
                order_book.add_bid(ref, int(row[5]), int(row[6]))
            else:
                order_book.add_ask(ref, int(row[5]), int(row[6]))
        elif order_type == 'E':
            order_book.execute_order(ref, int(row[6]))
        elif order_type == 'C':
            order_book.execute_order_with_price(ref, int(row[5]), int(row[6]))
        elif order_type == 'X':  # cancel
            order_book.cancel_order(ref, int(row[6]))
        elif order_type == 'D':  # delete
            order_book.delete_order(ref)
        elif order_type == 'U':  # update
            order_book.replace_order(ref, int(row[4]), int(row[5]), int(row[6]))
