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
        return (self.ask_book.get_quote() + self.bid_book.get_quote()) / 2

    def process_message(self, msg: FormattedMessage):
        price, shares = None, None
        if msg.type == 'AA':  # add Ask
            return self.add_ask(msg.ref, msg.price, msg.shares)
        if msg.type == 'AA2':  # add algo generated ask
            return self.add_ask(msg.ref, msg.price, msg.shares, False)
        elif msg.type == 'AB':  # ask bid
            return self.add_bid(msg.ref, msg.price, msg.shares)
        elif msg.type == 'AB2':  # ask algo generated bid
            return self.add_bid(msg.ref, msg.price, msg.shares, False)
        elif msg.type == 'E' or msg.type == 'C':
            return self.execute_order(msg.ref, msg.shares)
        elif msg.type == "MB":  # market buy
            return self.bid_book.execute_market_market(msg.ref, msg.shares)
        elif msg.type == "MS":  # market sell
            return self.ask_book.execute_market_market(msg.ref, msg.shares)
        elif msg.type == 'X':  # cancel
            self.cancel_order(msg.ref, msg.shares)
        elif msg.type == 'D':  # delete
            self.delete_order(msg.ref)
        elif msg.type == 'U':  # update
            self.replace_order(msg.ref, msg.new_ref, msg.price, msg.shares)
        return price, shares

    def add_bid(self, ref, price, shares, real=True):
        if price < self.ask_book.get_quote():
            self.bid_book.add_order(ref, price, shares, real)
            return None, None
        else:
            # cross the book, this can happen with both real (when algo order is added) and algo order
            return self.ask_book.execute_market_market(ref, shares)

    def add_ask(self, ref, price, shares, real=True):
        if price > self.bid_book.get_quote():
            self.ask_book.add_order(ref, price, shares, real)
            return None, None
        else:
            return self.bid_book.execute_market_market(ref, shares)

    def execute_order(self, ref, shares):
        if ref in self.ask_book or ref == -1:
            return self.ask_book.execute_order(ref, shares)
        elif ref in self.bid_book or ref == -2:
            return self.bid_book.execute_order(ref, shares, False)
        else:
            raise RuntimeError("Execution error - ref not exists")

    def cancel_order(self, ref: int, shares: int):
        if ref in self.ask_book:
            self.ask_book.cancel_order(ref, shares)
        elif ref in self.bid_book:
            self.bid_book.cancel_order(ref, shares)
        else:
            raise RuntimeError("Cancellation error - ref not exists")

    def delete_order(self, ref: int):
        if ref in self.ask_book:
            self.ask_book.delete_order(ref)
        elif ref in self.bid_book:
            self.bid_book.delete_order(ref)
        # else:
        #     raise RuntimeError("Deletion error - ref not exists")

    def replace_order(self, ref: int, new_ref:int, price: int, shares: int):
        if ref in self.ask_book:
            self.ask_book.replace_order(ref, new_ref, price, shares)
        elif ref in self.bid_book:
            self.bid_book.replace_order(ref, new_ref, price, shares)
        else:
            raise RuntimeError("Replacement error - ref not exists")
