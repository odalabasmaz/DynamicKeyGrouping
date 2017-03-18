import json


class Message:
    def toJson(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False)

    def toJsonIndented(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=2)
