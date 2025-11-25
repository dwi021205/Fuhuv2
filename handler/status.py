import discord
from discord.ext import tasks
import psutil
import platform
import time
from datetime import datetime
import pytz
import asyncio
import socket
import aiohttp
import sqlite3

from database import get_db
import logging

logger = logging.getLogger(__name__)
def format_uptime(seconds: float) -> str:
    if seconds is None:
        return "N/A"
    
    seconds = int(seconds)
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days} Days")
    if hours > 0:
        parts.append(f"{hours} Hour")
    if minutes > 0:
        parts.append(f"{minutes} Minutes")
    if seconds > 0 or not parts:
        parts.append(f"{seconds} Second")
        
    return " ".join(parts)

class StatusManager:
    def __init__(self, bot, config):
        self.bot = bot
        self.config = config
        self.db = get_db()
        self.log_channel_id = int(self.config.get('logbot', 0))
        self.jakarta_tz = pytz.timezone('Asia/Jakarta')
        self._last_message_id = None
        self._network_error_backoff_until = 0.0
        self._consecutive_failures = 0
        self._system_cache = None
        self._system_cache_deadline = 0.0
        self._system_cache_ttl = 30.0
        self._system_cache_lock = asyncio.Lock()
        self._premium_cache_value = 0
        self._premium_cache_deadline = 0.0
        self._premium_cache_ttl = 60.0
        self._premium_cache_lock = asyncio.Lock()

    def _collect_system_snapshot(self, bot_start_time):
        os_name = f"{platform.system()} {platform.release()}"
        ram_total = psutil.virtual_memory().total / (1024**3)
        ram_used = psutil.virtual_memory().used / (1024**3)
        ram_available = max(ram_total - ram_used, 0)

        disk_total = psutil.disk_usage('/').total / (1024**3)
        disk_used = psutil.disk_usage('/').used / (1024**3)
        disk_available = max(disk_total - disk_used, 0)

        os_uptime_seconds = time.time() - psutil.boot_time()
        bot_uptime_seconds = (time.time() - bot_start_time) if bot_start_time else 0

        return {
            'os_name': os_name,
            'ram_available': ram_available,
            'disk_available': disk_available,
            'os_uptime_seconds': os_uptime_seconds,
            'bot_uptime_seconds': bot_uptime_seconds,
        }

    async def _get_system_snapshot(self):
        now = time.monotonic()
        async with self._system_cache_lock:
            if self._system_cache and now < self._system_cache_deadline:
                return dict(self._system_cache)

        snapshot = await asyncio.to_thread(self._collect_system_snapshot, getattr(self.bot, 'start_time', None))

        async with self._system_cache_lock:
            self._system_cache = snapshot
            self._system_cache_deadline = time.monotonic() + self._system_cache_ttl

        return dict(snapshot)

    async def _get_premium_user_count(self) -> int:
        async with self._premium_cache_lock:
            now = time.monotonic()
            if now < self._premium_cache_deadline:
                return self._premium_cache_value

            attempts = 3
            delay = 0.25
            last_error = None

            for attempt in range(attempts):
                try:
                    if hasattr(self.db, "count_users_fast"):
                        result = await asyncio.to_thread(self.db.count_users_fast)
                    else:
                        result = await asyncio.to_thread(self.db.users.count_documents, {})
                    value = int(result) if result is not None else 0
                    self._premium_cache_value = max(value, 0)
                    self._premium_cache_deadline = time.monotonic() + self._premium_cache_ttl
                    return self._premium_cache_value
                except sqlite3.OperationalError as exc:
                    last_error = exc
                    if "locked" in str(exc).lower() and attempt < attempts - 1:
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 2.0)
                        continue
                    break
                except Exception as exc:
                    last_error = exc
                    break

            if last_error:
                logger.warning(
                    "Using cached premium user count (%s) due to DB error: %s",
                    self._premium_cache_value,
                    last_error,
                )
            fallback_ttl = min(30.0, self._premium_cache_ttl)
            self._premium_cache_deadline = time.monotonic() + fallback_ttl
            return self._premium_cache_value

    async def get_all_status_data(self):
        snapshot = await self._get_system_snapshot()
        os_name = snapshot['os_name']
        ram_available = snapshot['ram_available']
        disk_available = snapshot['disk_available']
        os_uptime = format_uptime(snapshot['os_uptime_seconds'])
        bot_uptime = format_uptime(snapshot['bot_uptime_seconds'])

        premium_users = await self._get_premium_user_count()

        return {
            'os_name': os_name,
            'ram_available': ram_available,
            'disk_available': disk_available,
            'os_uptime': os_uptime,
            'bot_uptime': bot_uptime,
            'premium_users': premium_users
        }

    @tasks.loop(minutes=2)
    async def status_updater(self):
        if not self.log_channel_id:
            return

        channel = self.bot.get_channel(self.log_channel_id)
        if not channel:
            return

        if time.monotonic() < self._network_error_backoff_until:
            return

        try:
            async with asyncio.timeout(30.0):
                status_data = await self.get_all_status_data()
        except asyncio.TimeoutError:
            logger.error("Status update timeout - skipping this cycle")
            return
        except Exception as e:
            logger.exception("Error gathering status data: %s", e)
            return

        embed = discord.Embed(
            title="Bots Status",
            description="Realtime status bot discord (if online)",
            color=self.config.get('global_color'),
            timestamp=datetime.now(self.jakarta_tz)
        )

        os_details_value = (
            f"**<:dot:1426148484146270269> Operating Sistem** : {status_data['os_name']}\n"
            f"**<:dot:1426148484146270269> RAM Avaliable** : {status_data['ram_available']:.2f} GB\n"
            f"**<:dot:1426148484146270269> Memory Avaliable** : {status_data['disk_available']:.2f} GB\n"
            f"**<:dot:1426148484146270269> OS Uptime** : {status_data['os_uptime']}\n"
            f"**<:dot:1426148484146270269> Bot Uptime** : {status_data['bot_uptime']}"
        )
        embed.add_field(name="OS Details <:computer:1426164290490073208>", value=os_details_value, inline=False)

        user_info_value = f"**<:dot:1426148484146270269> Premium User** : {status_data['premium_users']}"
        embed.add_field(name="User Info <:user:1426148393092386896>", value=user_info_value, inline=False)

        img_url = self.config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)

        embed.set_footer(
            text=f"{self.config.get('global_footer', '')}",
            icon_url=self.config.get('global_footer_icon', '')
        )

        await self._send_or_edit_status(channel, embed)
    def _register_network_failure(self, stage: str, error: Exception, base: float = 60.0):
        self._consecutive_failures += 1
        backoff = min(600, base * max(1, self._consecutive_failures))
        detail = f"{type(error).__name__}: {error}" if error else "unknown"
        logger.error("Status updater network error during %s: %s; backing off %.0fs", stage, detail, backoff)
        self._network_error_backoff_until = time.monotonic() + backoff

    def _handle_http_exception(self, stage: str, exc: discord.HTTPException) -> bool:
        code = getattr(exc, 'code', None)
        if code == 10008 or exc.status == 404:
            return True
        self._consecutive_failures += 1
        if exc.status == 429:
            retry_after = float(getattr(exc, 'retry_after', 60) or 60)
            logger.error("Status updater rate limited during %s; backing off %.0fs", stage, retry_after)
            self._network_error_backoff_until = time.monotonic() + retry_after
        elif exc.status == 503:
            backoff_time = 300
            logger.error("Status updater service unavailable during %s; backing off %ds", stage, backoff_time)
            self._network_error_backoff_until = time.monotonic() + backoff_time
        else:
            logger.exception("Status updater HTTP error during %s: %s", stage, exc)
            self._network_error_backoff_until = time.monotonic() + 120.0
        return False

    async def _send_or_edit_status(self, channel: discord.TextChannel, embed: discord.Embed):
        message_id = self._last_message_id
        if not message_id:
            log_config = await asyncio.to_thread(self.db.get_status_log_config)
            message_id = log_config.get('logmessageid') if log_config else None

        try:
            target_message = None
            message_missing = False
            if message_id:
                try:
                    async with asyncio.timeout(10.0):
                        target_message = await channel.fetch_message(message_id)
                except discord.NotFound:
                    message_missing = True
                    target_message = None
                except discord.HTTPException as e:
                    if self._handle_http_exception("fetch", e):
                        message_missing = True
                    else:
                        return
                except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerTimeoutError, asyncio.TimeoutError, socket.gaierror) as e:
                    self._register_network_failure("fetch", e, base=90.0)
                    return

            if target_message and not message_missing:
                try:
                    async with asyncio.timeout(10.0):
                        await target_message.edit(embed=embed)
                except discord.HTTPException as e:
                    if self._handle_http_exception("edit", e):
                        message_missing = True
                    else:
                        return
                except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerTimeoutError, asyncio.TimeoutError, socket.gaierror) as e:
                    self._register_network_failure("edit", e)
                    return

            if not target_message or message_missing:
                try:
                    async with asyncio.timeout(10.0):
                        sent = await channel.send(embed=embed)
                    await asyncio.to_thread(self.db.update_status_log_config, sent.id)
                    self._last_message_id = sent.id
                except discord.HTTPException as e:
                    if self._handle_http_exception("send", e):
                        return
                    return
                except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerTimeoutError, asyncio.TimeoutError, socket.gaierror) as e:
                    self._register_network_failure("send", e)
                    return

            self._network_error_backoff_until = 0.0
            self._consecutive_failures = 0

        except discord.Forbidden:
            logger.error("Status updater error: Missing permission in channel %s", self.log_channel_id)
            self._network_error_backoff_until = time.monotonic() + 300.0
        except Exception as e:
            logger.exception("Status updater unexpected error: %s", e)
            self._network_error_backoff_until = time.monotonic() + 60.0

    @status_updater.before_loop
    async def before_status_updater(self):
        await self.bot.wait_until_ready()

_manager_instance = None

def init_status_manager(bot, config):
    global _manager_instance
    if _manager_instance is None:
        _manager_instance = StatusManager(bot, config)

def get_status_manager():
    return _manager_instance
