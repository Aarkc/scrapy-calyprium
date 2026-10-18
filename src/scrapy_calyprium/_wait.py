"""Waiting inside a Scrapy coroutine (a middleware method or a spider callback) without blocking the
reactor or the rest of the crawl."""
import asyncio


async def sleep(seconds: float) -> None:
    """asyncio's sleep under the asyncio reactor (the platform's), Twisted's ``deferLater`` under
    any other: an ``async def`` callback runs as an asyncio task on the first and as a Deferred
    coroutine on the second, and each can only await its own kind."""
    if seconds <= 0:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        from twisted.internet import reactor
        from twisted.internet.task import deferLater

        await deferLater(reactor, seconds, lambda: None)
        return
    await asyncio.sleep(seconds)
