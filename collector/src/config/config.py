class BaseConfig:

    COLLECTOR_PATH = "/collect"
    V_API = "v1"
    TESTING = False
    DEBUG = False


class DevConfig(BaseConfig):

    DEBUG = True


class ProdConfig(BaseConfig):

    ...


class TestConfig(BaseConfig):

    ...
