
import os.path
from datetime import datetime


from MessageHandler import Tokenizer
from MessageHandler import parse_message
import Message


def format_message(msg):
    # type, tracking, ref, timestamp, acton (buy / sell / executed/), price, shares
    if isinstance(msg, Message.OrderAdd) or isinstance(msg, Message.OrderAddMpid):
        return ['A', msg.tracking, msg.ref, msg.timestamp, 1 if msg.buy_sell == b'B' else 0, msg.price, msg.shares]
    elif isinstance(msg, Message.OrderDelete):
        return ['D', msg.tracking, msg.ref, msg.timestamp, '', '', '']
    elif isinstance(msg, Message.OrderCancel):
        return ['X', msg.tracking, msg.ref, msg.timestamp, '', '', msg.shares]
    elif isinstance(msg, Message.OrderReplace):
        return ['U', msg.tracking, msg.ref, msg.timestamp, msg.new_ref, msg.price, msg.shares]
    elif isinstance(msg, Message.OrderExecuted):
        return ['E', msg.tracking, msg.ref, msg.timestamp, msg.match, '', msg.shares]
    elif isinstance(msg, Message.OrderExecutedWithPrice):
        return ['C', msg.tracking, msg.ref, msg.timestamp, msg.match, msg.price, msg.shares]
    elif isinstance(msg, Message.NonCrossTrade):
        return ['P', msg.tracking, msg.ref, msg.timestamp, msg.match, msg.price, msg.shares]
    return None


def parse_and_save(src, out_path):
    if not os.path.exists(src):
        raise RuntimeError("Source file not exists")
    if not os.path.exists(out_path):
        raise RuntimeError("Out path not exists")
    date = datetime.strptime(str(os.path.basename(src).split("-")[0][1:]), "%d%m%y")
    date = datetime.strftime(date, "%Y%m%d")

    to_save = {}
    record = {}
    counter = 0
    with Tokenizer(src) as reader:
        while True:
            msg = parse_message(reader.get_message())
            # counter += 1
            # if counter > 1E5:
            #     break

            if isinstance(msg, Message.StockDirectory):
                to_save[msg.stock] = open("%s-%s.csv" % (msg.stock.decode().strip(), date), "w")
            else:
                if isinstance(msg, Message.OrderAdd) or isinstance(msg, Message.OrderAddMpid):
                    record[msg.ref] = msg.stock
                elif isinstance(msg, Message.OrderReplace):
                    record[msg.new_ref] = record[msg.ref]
                tmp = format_message(msg)
                if tmp is None:
                    continue

                if not(isinstance(msg, Message.OrderAdd) or isinstance(msg, Message.OrderAddMpid)
                       or isinstance(msg, Message.NonCrossTrade)):
                    stock = record[msg.ref]
                else:
                    stock = msg.stock
                to_save[stock].write(",".join([str(x) for x in tmp]) + "\n")

            print("%d\r" % counter, end="", flush=True)
            if isinstance(msg, Message.SystemEvent) and msg.event == b'C':
                break

    for f in to_save.values():
        f.close()
