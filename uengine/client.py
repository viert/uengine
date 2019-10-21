import requests


class UEngineClient:

    def __init__(self, baseurl, api_token):
        self.baseurl = baseurl
        self.set_token(api_token)

    def set_token(self, api_token):
        self.api_token = api_token

    def request(self, method, path, json=None):
        url = f"{self.baseurl}{path}"
        headers = {"X-Api-Auth-Token": self.api_token}
        if method == "GET":
            return requests.get(url, headers=headers)
        return requests.request(method, url, headers=headers, json=json)

    def get(self, path):
        return self.request("GET", path)

    def delete(self, path):
        return self.request("DELETE", path)

    def post(self, path, json=None):
        return self.request("POST", path, json)

    def put(self, path, json=None):
        return self.request("PUT", path, json)

    def patch(self, path, json=None):
        return self.request("PATCH", path, json)
