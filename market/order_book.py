"""
OrderBook implementation using SortedList
"""
from market.book import Book
from market.elements import ask_comparators, bid_comparators, FormattedMessage


class OrderBook:
    def __init__(self):
        self.ask_book = Book(ask_comparators)
        self.bid_book = Book(bid_comparators)

    def get_spread(self):
        return self.ask_book.get_quote() - self.bid_book.get_quote()

    def get_mid_price(self):
        return int((self.ask_book.get_quote() + self.bid_book.get_quote()) / 2)

    def process_message(self, msg: FormattedMessage):
        price, shares = None, None
        if msg.type == 'AA':  # add Ask
            return self.add_ask(msg.ref, msg.price, msg.shares, real=True)
        if msg.type == 'AA2':  # add algo generated ask
            return self.add_ask(msg.ref, msg.price, msg.shares, real=False)
        elif msg.type == 'AB':  # ask bid
            return self.add_bid(msg.ref, msg.price, msg.shares, real=True)
        elif msg.type == 'AB2':  # ask algo generated bid
            return self.add_bid(msg.ref, msg.price, msg.shares, real=False)
        elif msg.type == 'EA':  # execution or execution with price
            return self.ask_book.execute_order(msg.ref, msg.shares, True)
        elif msg.type == 'EB':  # execution or execution with price
            return self.bid_book.execute_order(msg.ref, msg.shares, False)
        elif msg.type == "MB":  # market buy
            return self.bid_book.execute_market_market(msg.ref, msg.shares)
        elif msg.type == "MS":  # market sell
            return self.ask_book.execute_market_market(msg.ref, msg.shares)
        elif msg.type == 'XA':  # cancel
            self.ask_book.cancel_order(msg.ref, msg.shares)
        elif msg.type == 'XB':  # cancel
            self.bid_book.cancel_order(msg.ref, msg.shares)
        elif msg.type == 'DA':  # delete
            self.ask_book.delete_order(msg.ref)
        elif msg.type == 'DB':  # delete
            self.bid_book.delete_order(msg.ref)
        elif msg.type == 'UA':  # update
            self.ask_book.replace_order(msg.ref, msg.new_ref, msg.price, msg.shares)
        elif msg.type == 'UB':  # update
            self.bid_book.replace_order(msg.ref, msg.new_ref, msg.price, msg.shares)
        else:
            print("Unrecognized message type: ", msg.type)
        return price, shares

    def add_bid(self, ref, price, shares, real):
        if price < self.ask_book.get_quote():
            self.bid_book.add_order(ref, price, shares, real)
            return []
        else:
            # cross the book, this can happen with both real (when algo order is added) and algo order
            return self.ask_book.execute_market_market(ref, shares)

    def add_ask(self, ref, price, shares, real):
        if price > self.bid_book.get_quote():
            self.ask_book.add_order(ref, price, shares, real)
            return []
        else:
            return self.bid_book.execute_market_market(ref, shares)

    def execute_order(self, ref, shares):
        if ref in self.ask_book or ref == -1:
            return self.ask_book.execute_order(ref, shares)
        elif ref in self.bid_book or ref == -2:
            return self.bid_book.execute_order(ref, shares, False)
        else:
            raise RuntimeError("Execution error - ref not exists")
