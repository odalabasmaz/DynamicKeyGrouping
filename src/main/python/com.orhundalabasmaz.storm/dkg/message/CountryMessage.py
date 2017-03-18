from dkg.message.Message import Message


class CountryMessage(Message):
    def __init__(self, country):
        self.country = country
        pass
