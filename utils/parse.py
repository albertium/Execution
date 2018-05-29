
import os.path
from datetime import datetime
import time


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

    get_name = lambda x: os.path.join(out_path, "%s-%s.csv" % (x.decode().strip(), date))

    to_save = {}
    record = {}
    counter = 0
    reset = 0
    start = time.clock()
    with Tokenizer(src) as reader:
        while True:
            msg = parse_message(reader.get_message())
            counter += 1
            reset += 1
            # if counter > 1E5:
            #     break

            if isinstance(msg, Message.SystemEvent) and msg.event == b'C':
                break

            if isinstance(msg, Message.StockDirectory):
                with open(get_name(msg.stock), "w") as f:
                    pass
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
                if stock in to_save:
                    to_save[stock].append(",".join([str(x) for x in tmp]))
                else:
                    to_save[stock] = [",".join([str(x) for x in tmp])]

            if reset >= 2E5:
                delta = time.clock() - start
                print("\r%d (elapsed: %dmin / rate: %d)" % (counter, delta / 60, counter / delta), end="", flush=True)
                for k, v in to_save.items():
                    with open(get_name(k), "a") as f:
                        f.write("\n".join(v) + "\n")
                to_save.clear()
                reset = 0

