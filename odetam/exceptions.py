class DetaError(BaseException):
    pass


class ItemNotFound(DetaError):
    pass


class InvalidDetaQuery(DetaError):
    pass


class InvalidKey(DetaError):
    pass
