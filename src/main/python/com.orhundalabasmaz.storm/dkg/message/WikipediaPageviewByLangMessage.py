from dkg.message.Message import Message


class WikipediaPageviewByLangMessage(Message):
    def __init__(self, lang, page, n, m):
        self.lang = lang
        self.page = page
        self.n = n
        self.m = m
        pass
