import logging
import sys

import pytest

E2E_LOGGER_NAME = "improvements.tests.e2e"


@pytest.fixture(autouse=True)
def e2e_test_logging(request):
    if request.node.fspath.basename != "test_improvements_e2e.py":
        yield
        return

    test_logger = logging.getLogger(E2E_LOGGER_NAME)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.INFO)
    test_logger.propagate = False
    yield
    test_logger.removeHandler(handler)
