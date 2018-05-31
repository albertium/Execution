
import csv
import time
from market.order_book import OrderBook


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

            if counter % 1000 == 0:
                print("\rElapsed: %ds / %d" % (time.clock() - start, counter), end='', flush=True)
    print("\rTotal: %ds / %d\n" % (time.clock() - start, counter), end='', flush=True)
    print("%d %d" % (len(order_book.ask_book.pool), len(order_book.bid_book.pool)))

    count = 0
    for level in order_book.ask_book.levels:
        for order in level.queue:
            count += order.valid
    print("\n%d" % count)