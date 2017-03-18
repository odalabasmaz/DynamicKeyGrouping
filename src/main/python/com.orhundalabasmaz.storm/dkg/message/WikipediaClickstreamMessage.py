from dkg.message.Message import Message


class WikipediaClickstreamMessage(Message):
    def __init__(self, prev, curr, type, n):
        self.prev = prev
        self.curr = curr
        self.type = type
        self.n = n
        pass
