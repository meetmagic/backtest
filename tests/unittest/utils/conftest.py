from pytest import fixture
from utils.env_setting import Settings

@fixture(scope="session", autouse=False)
def env():
    env = Settings("dev")
    return env

@fixture(scope="session", autouse=False)
def mongodb(env):
    return env.mongodb