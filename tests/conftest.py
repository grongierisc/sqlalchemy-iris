from sqlalchemy.dialects import registry
import pytest
import time

from sqlalchemy.testing.plugin.plugin_base import pre

from testcontainers.iris import IRISContainer

registry.register("iris.iris", "sqlalchemy_iris.iris", "IRISDialect_iris")
registry.register("iris.emb", "sqlalchemy_iris.embedded", "IRISDialect_emb")
registry.register(
    "iris.intersystems", "sqlalchemy_iris.intersystems", "IRISDialect_intersystems"
)
registry.register(
    "iris.irisasync", "sqlalchemy_iris.irisasync", "IRISDialect_irisasync"
)

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa

original_pytest_addoption = pytest_addoption


def pytest_addoption(parser):
    original_pytest_addoption(parser)

    group = parser.getgroup("iris")

    group.addoption(
        "--container",
        action="store",
        default=None,
        type=str,
        help="Docker image with IRIS",
    )


@pre
def start_container(opt, file_config):
    global iris
    iris = None
    if not opt.container:
        return

    print("Starting IRIS container:", opt.container)
    iris = IRISContainer(
        opt.container,
        username="sqlalchemy",
        password="sqlalchemy",
        namespace="TEST",
    )
    # iris.start()
    # print("dburi:", iris.get_connection_url())
    # opt.dburi = [iris.get_connection_url()]
    opt.dburi = ["iris+intersystems://SuperUser:SYS@localhost:51773/USER"]


def pytest_unconfigure(config):
    global iris
    if iris:
        print("Stopping IRIS container")
        # iris.stop()
