
import struct
import csv


def parse_and_save(infile, outfile):
    with open(infile, "rb") as f:
        data = f.read()
        idx = 0
        counter = 0
        output = []
        while idx < len(data) + 1:
            size = data[idx + 1]
            idx += 2 + size
            if idx >= len(data):
                break
            msg = data[idx - size: idx]
            msg_type = chr(msg[0])
            if msg_type == 'A' or msg_type == 'F':
                locate, tracking, timestamp, ref, buy_sell, shares, stock, price = struct.unpack("!HH6sQcI8sI", msg[1: 36])
                if locate == 14:  # 14 is AAPL
                    timestamp = int.from_bytes(timestamp, "big")
                    output.append(['A', ref, timestamp, 1 if buy_sell == b'B' else 0, price, shares])
            elif msg_type == 'E' or msg_type == 'C':
                locate, tracking, timestamp, ref, shares, match = struct.unpack("!HH6sQIQ", msg[1:31])
                if locate == 14:
                    timestamp = int.from_bytes(timestamp, "big")
                    output.append(['E', ref, timestamp, match, '', shares])
            elif msg_type == 'X':
                locate, tracking, timestamp, ref, shares = struct.unpack("!HH6sQI", msg[1:])
                if locate == 14:
                    timestamp = int.from_bytes(timestamp, "big")
                    output.append(['X', ref, timestamp, '', '', shares])
            elif msg_type == 'D':
                locate, tracking, timestamp, ref = struct.unpack("!HH6sQ", msg[1:])
                if locate == 14:
                    timestamp = int.from_bytes(timestamp, "big")
                    output.append(['D', ref, timestamp, '', '', ''])
            elif msg_type == 'U':
                locate, tracking, timestamp, ref, new_ref, shares, price = struct.unpack("!HH6sQQII", msg[1:])
                if locate == 14:
                    timestamp = int.from_bytes(timestamp, "big")
                    output.append(['U', ref, timestamp, new_ref, price, shares])

    with open(outfile, "w") as f:
        text = [",".join([str(elem) for elem in row]) for row in output]
        f.write("\n".join(text) + "\n")


def preprocess_data(filename):
    output = []
    record = {}
    with open(filename, "r") as f:
        reader = csv.reader(f)
        for msg in reader:
            if msg[0] == 'A':
                label = 'B' if msg[3] == '1' else 'A'
                msg[0] += label
                record[msg[1]] = label
            if msg[0] == 'E' or msg[0] == 'X':
                msg[0] += record[msg[1]]
            if msg[0] == 'D':
                msg[0] += record[msg[1]]
                del record[msg[1]]
            if msg[0] == 'U':
                msg[0] += record[msg[1]]
                record[msg[3]] = record[msg[1]]
                del record[msg[1]]
            output.append(msg)

    with open(filename[:-4] + "-v2.csv", "w") as f:
        f.write("\n".join(",".join(row) for row in output) + "\n")
