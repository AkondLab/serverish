import os
import pytest

@pytest.fixture(scope="session")
def nats_host():
    #  CI or local
    if os.environ.get('CI'):  # 'CI' is a default env variable on GitHub Actions and other CI systems
        return "nats"  # NATS server on CI environment
    else:
        return "localhost"  # NATS server on local environment

@pytest.fixture(scope="session")
def nats_port():
        return 4222  # NATS port (the same on CI and local environment)
