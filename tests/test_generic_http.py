import asyncio
import json
import time
from unittest.mock import patch

import pytest

from channels.generic.http import AsyncHttpConsumer
from channels.testing import HttpCommunicator


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer():
    """
    Tests that AsyncHttpConsumer is implemented correctly.
    """

    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            data = json.loads(body.decode("utf-8"))
            await self.send_response(
                200,
                json.dumps({"value": data["value"]}).encode("utf-8"),
                headers={b"Content-Type": b"application/json"},
            )

    app = TestConsumer()

    # Open a connection
    communicator = HttpCommunicator(
        app,
        method="POST",
        path="/test/",
        body=json.dumps({"value": 42, "anything": False}).encode("utf-8"),
    )
    response = await communicator.get_response()
    assert response["body"] == b'{"value": 42}'
    assert response["status"] == 200
    assert response["headers"] == [(b"Content-Type", b"application/json")]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_error():
    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            raise AssertionError("Error correctly raised")

    communicator = HttpCommunicator(TestConsumer(), "GET", "/")
    with pytest.raises(AssertionError) as excinfo:
        await communicator.get_response(timeout=0.05)

    assert str(excinfo.value) == "Error correctly raised"


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_per_scope_consumers():
    """
    Tests that a distinct consumer is used per scope, with AsyncHttpConsumer as
    the example consumer class.
    """

    class TestConsumer(AsyncHttpConsumer):
        def __init__(self):
            super().__init__()
            self.time = time.time()

        async def handle(self, body):
            body = f"{self.__class__.__name__} {id(self)} {self.time}"

            await self.send_response(
                200,
                body.encode("utf-8"),
                headers={b"Content-Type": b"text/plain"},
            )

    app = TestConsumer.as_asgi()

    # Open a connection
    communicator = HttpCommunicator(app, method="GET", path="/test/")
    response = await communicator.get_response()
    assert response["status"] == 200

    # And another one.
    communicator = HttpCommunicator(app, method="GET", path="/test2/")
    second_response = await communicator.get_response()
    assert second_response["status"] == 200

    assert response["body"] != second_response["body"]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_async_http_consumer_future():
    """
    Regression test for channels accepting only coroutines. The ASGI specification
    states that the `receive` and `send` arguments to an ASGI application should be
    "awaitable callable" objects. That includes non-coroutine functions that return
    Futures.
    """

    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            await self.send_response(
                200,
                b"42",
                headers={b"Content-Type": b"text/plain"},
            )

    app = TestConsumer()

    # Ensure the passed functions are specifically coroutines.
    async def coroutine_app(scope, receive, send):
        async def receive_coroutine():
            return await asyncio.ensure_future(receive())

        async def send_coroutine(*args, **kwargs):
            return await asyncio.ensure_future(send(*args, **kwargs))

        await app(scope, receive_coroutine, send_coroutine)

    communicator = HttpCommunicator(coroutine_app, method="GET", path="/")
    response = await communicator.get_response()
    assert response["body"] == b"42"
    assert response["status"] == 200
    assert response["headers"] == [(b"Content-Type", b"text/plain")]

    # Ensure the passed functions are "Awaitable Callables" and NOT coroutines.
    async def awaitable_callable_app(scope, receive, send):
        def receive_awaitable_callable():
            return asyncio.ensure_future(receive())

        def send_awaitable_callable(*args, **kwargs):
            return asyncio.ensure_future(send(*args, **kwargs))

        await app(scope, receive_awaitable_callable, send_awaitable_callable)

    # Open a connection
    communicator = HttpCommunicator(awaitable_callable_app, method="GET", path="/")
    response = await communicator.get_response()
    assert response["body"] == b"42"
    assert response["status"] == 200
    assert response["headers"] == [(b"Content-Type", b"text/plain")]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_error_logging():
    """Regression test for error logging."""

    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            raise AssertionError("Error correctly raised")

    communicator = HttpCommunicator(TestConsumer(), "GET", "/")
    with patch("channels.generic.http.logger.error") as mock_logger_error:
        try:
            await communicator.get_response(timeout=0.05)
        except AssertionError:
            pass
        args, _ = mock_logger_error.call_args
        assert "Error in handle()" in args[0]
        assert "AssertionError: Error correctly raised" in args[0]


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_error_handling_and_send_response():
    """Regression test to check error handling."""

    class TestConsumer(AsyncHttpConsumer):
        async def handle(self, body):
            raise AssertionError("Error correctly raised")

    communicator = HttpCommunicator(TestConsumer(), "GET", "/")
    with patch.object(AsyncHttpConsumer, "send_response") as mock_send_response:
        try:
            await communicator.get_response(timeout=0.05)
        except AssertionError:
            pass
        mock_send_response.assert_called_once_with(500, b"Internal Server Error")
