import requests


class UEngineClientError(Exception):
    def __init__(self, message, status_code, path):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.path = path

    def __str__(self):
        return f"UEngineClientError status_code={self.status_code}, path={self.path} message:\n  {self.message}"

    def __repr__(self):
        return self.__str__()


class UEngineClient:

    RESTRICTED_OPTIONS = ('json', 'headers',)

    def __init__(self, baseurl, api_token, api_prefix=None, options=None):
        self.baseurl = baseurl
        self.api_prefix = api_prefix
        if self.api_prefix and self.api_prefix.endswith("/"):
            self.api_prefix = self.api_prefix[:-1]
        self.set_token(api_token)
        self.set_options(options)

    def set_options(self, options=None):
        self.options = options
        if not self.options:
            self.options = {}
        for opt in self.RESTRICTED_OPTIONS:
            if opt in self.options:
                del(self.options[opt])

    def set_token(self, api_token):
        self.api_token = api_token

    def request(self, method, path, json=None):
        if not path.startswith("/"):
            if self.api_prefix is None:
                raise ValueError(
                    "can't use path without leading slash when API prefix is not defined")
            path = f"{self.api_prefix}/{path}"

        url = f"{self.baseurl}{path}"
        headers = {"X-Api-Auth-Token": self.api_token}
        if method == "GET":
            response = requests.get(url, headers=headers, **self.options)
        else:
            response = requests.request(
                method, url, headers=headers, json=json, **self.options)

        resp_content = response.json()

        if response.status_code == 200:
            return resp_content

        err = resp_content.get("error") or resp_content
        raise UEngineClientError(err, response.status_code, path)

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
