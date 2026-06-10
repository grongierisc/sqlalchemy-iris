import pytest

from sqlalchemy import exc
from sqlalchemy.engine import make_url
from sqlalchemy.testing import fixtures

from sqlalchemy_iris.embedded import IRISDialect_emb


def _connect_opts(url):
    args, opts = IRISDialect_emb().create_connect_args(make_url(url))
    assert args == []
    return opts


class EmbeddedURLTest(fixtures.TestBase):
    @pytest.mark.parametrize(
        ("url", "namespace"),
        [
            ("iris+emb://", "USER"),
            ("iris+emb:///", "USER"),
            ("iris+emb://SAMPLES", "SAMPLES"),
            ("iris+emb:///SAMPLES", "SAMPLES"),
        ],
    )
    def test_embedded_url_namespace(self, url, namespace):
        opts = _connect_opts(url)

        assert opts["mode"] == "embedded"
        assert opts["namespace"] == namespace

    def test_embedded_url_namespace_with_path_option(self):
        opts = _connect_opts("iris+emb://SAMPLES?path=/opt/iris")

        assert opts["namespace"] == "SAMPLES"
        assert opts["path"] == "/opt/iris"

    @pytest.mark.parametrize(
        "url",
        [
            "iris+emb://localhost:1972/USER",
            "iris+emb://user:pass@USER",
        ],
    )
    def test_embedded_url_rejects_remote_connection_parts(self, url):
        with pytest.raises(exc.ArgumentError, match="local-only"):
            _connect_opts(url)

    def test_embedded_url_rejects_duplicate_namespace(self):
        with pytest.raises(exc.ArgumentError, match="not both"):
            _connect_opts("iris+emb://SAMPLES/USER")
