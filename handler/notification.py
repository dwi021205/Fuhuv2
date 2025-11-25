import asyncio
import contextlib
import logging
from typing import Any, Dict, Optional

import discord

logger = logging.getLogger(__name__)


class NotificationDispatcher:
    def __init__(self, bot: discord.Client, rate_limit_per_sec: int = 4):
        self.bot = bot
        self.rate_limit = max(1, int(rate_limit_per_sec))
        self._interval = 1.0 / self.rate_limit
        self._queue: Optional[asyncio.Queue] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def start(self):
        if self._running:
            return
        self._loop = asyncio.get_running_loop()
        if self._queue is None:
            self._queue = asyncio.Queue()
        self._running = True
        self._task = asyncio.create_task(self._worker(), name="NotificationDispatcher")
        logger.info("NotificationDispatcher started (rate=%s/sec)", self.rate_limit)

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None
        if self._queue:
            while not self._queue.empty():
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
        logger.info("NotificationDispatcher stopped")

    async def enqueue_dm(self, payload: Dict[str, Any]) -> bool:
        if not self._queue or not self._loop or not self._running:
            return False
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        coro = self._queue.put(payload)
        if current_loop is self._loop:
            await coro
        else:
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)
            if current_loop:
                await asyncio.wrap_future(future, loop=current_loop)
            else:
                await asyncio.to_thread(future.result)
        return True

    async def _worker(self):
        assert self._queue is not None
        while self._running:
            try:
                payload = await self._queue.get()
                await self._dispatch(payload)
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("NotificationDispatcher worker error")
                await asyncio.sleep(self._interval)

    async def _dispatch(self, payload: Dict[str, Any]):
        user_id = payload.get("user_id")
        if not user_id:
            return
        bot_loop = getattr(self.bot, "loop", None)
        if not bot_loop or bot_loop.is_closed():
            return

        async def _send():
            try:
                user = await self.bot.fetch_user(int(user_id))
                if not user:
                    return
                content = payload.get("content")
                embed = payload.get("embed")
                view = payload.get("view")
                await user.send(content=content, embed=embed, view=view)
            except (discord.NotFound, discord.Forbidden):
                logger.warning("NotificationDispatcher: cannot DM user %s", user_id)
            except Exception:
                logger.exception("NotificationDispatcher send error user=%s", user_id)

        future = asyncio.run_coroutine_threadsafe(_send(), bot_loop)
        await asyncio.wrap_future(future, loop=self._loop)


_dispatcher_instance: Optional[NotificationDispatcher] = None


def init_notification_dispatcher(bot: discord.Client, rate_limit_per_sec: int = 4) -> NotificationDispatcher:
    global _dispatcher_instance
    if _dispatcher_instance is None:
        _dispatcher_instance = NotificationDispatcher(bot, rate_limit_per_sec)
    return _dispatcher_instance


def get_notification_dispatcher() -> Optional[NotificationDispatcher]:
    return _dispatcher_instance
