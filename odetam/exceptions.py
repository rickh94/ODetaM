class DetaError(BaseException):
    pass


class ItemNotFound(DetaError):
    pass


class NoProjectKey(DetaError):
    pass


class InvalidDetaQuery(DetaError):
    pass
