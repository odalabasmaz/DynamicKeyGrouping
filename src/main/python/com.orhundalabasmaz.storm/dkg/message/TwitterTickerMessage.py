from dkg.message.Message import Message


class TwitterTickerMessage(Message):
    def __init__(self, timestamp, ticker):
        self.timestamp = timestamp
        self.ticker = ticker
        pass
