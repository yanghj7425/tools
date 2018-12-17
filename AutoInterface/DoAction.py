# -*- coding: utf-8 -*-
import requests
import json


class DoAction:
    def __init__(self):
        self.baseurl = 'http://127.0.0.1:8080/iPB_gab/doAction'
        self.headers = {}
        self.data = {}
        self.url = None

    def setHeaders(self, header):
        self.headers = header

    def setData(self, data):
        self.data = data

    # defined http post method
    def post(self):
        try:
            response = requests.post(
                self.baseurl, headers=self.headers, data=self.data)
            return response
        except TimeoutError:
            return None


doAction = DoAction()
doAction.setData(
    '''PFJlcXVlc3QgYWN0aW9uPSduZXdzLmFjdGlvbi5OZXdzTWFuYWdlcltxdWVyeUNoYW5uZWxOZXdzSXRlbUluZm9dJyByZXF1ZXN0PSdKU09OJyByZXNwb25zZT0nSlNPTicgYXJyYXk9Jycgbm9oZWFkPSd0cnVlJz4gPERhdGE+eyJDSEFOTkVMVFlQRSI6IjIiLCJVTklUX0lEIjoiMGVjNDEzODZlNjY4MTFlOGIwZDRjOGQzZmYzNzk0Y2MiLCJwU3RhcnQiOiIxIiwicFNpemUiOiI0In08L0RhdGE+PC9SZXF1ZXN0Pg==
''')

resp = doAction.post()
status = resp.status_code
print(status)
ret = json.loads(resp.text)
print(ret)