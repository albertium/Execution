
from MessageHandler import Tokenizer

with Tokenizer("data/S020117-v50-bx.txt.gz") as reader:
    for i in range(20):
        print(reader.get_message())
