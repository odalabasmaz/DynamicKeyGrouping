from dkg.message.Message import Message


class WikipediaPageviewMessage(Message):
    def __init__(self, timestamp, page):
        self.timestamp = timestamp
        self.page = page
        pass
