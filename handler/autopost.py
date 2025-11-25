import asyncio
import contextlib
import json
import gc
import sys
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Tuple, Awaitable
import time
import random
import copy
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import aiohttp
from aiohttp_socks import ProxyConnector
import base64
import uuid
import ctypes
from urllib.parse import urlparse

from database import get_db
sys.setrecursionlimit(1000)


logger = logging.getLogger(__name__)
logger.handlers.clear()
logger.addHandler(logging.NullHandler())
logger.setLevel(logging.ERROR)
logger.propagate = False


async def _run_gc_in_thread(generation: int = 1):
    try:
        await asyncio.to_thread(gc.collect, generation)
    except Exception:
        pass

DISCORD_VERSIONS = [
    {'version': '223.9', 'build': 223009, 'versionCode': 223409},
    {'version': '222.11', 'build': 222011, 'versionCode': 222411},
    {'version': '221.14', 'build': 221014, 'versionCode': 221414},
    {'version': '220.15', 'build': 220015, 'versionCode': 220415},
    {'version': '219.16', 'build': 219016, 'versionCode': 219416}
]

DEVICE_CONFIGS = [
    {'model': 'Samsung SM-G998B', 'brand': 'samsung', 'android_version': '14', 'sdk': '34'},
    {'model': 'Samsung SM-G991B', 'brand': 'samsung', 'android_version': '14', 'sdk': '34'},
    {'model': 'Samsung SM-G996B', 'brand': 'samsung', 'android_version': '13', 'sdk': '33'},
    {'model': 'Samsung SM-S911B', 'brand': 'samsung', 'android_version': '14', 'sdk': '34'},
    {'model': 'Samsung SM-S918B', 'brand': 'samsung', 'android_version': '14', 'sdk': '34'},
    {'model': 'Google Pixel 8 Pro', 'brand': 'Google', 'android_version': '14', 'sdk': '34'},
    {'model': 'Google Pixel 8', 'brand': 'Google', 'android_version': '14', 'sdk': '34'},
    {'model': 'Google Pixel 7 Pro', 'brand': 'Google', 'android_version': '14', 'sdk': '34'},
    {'model': 'OnePlus 11', 'brand': 'OnePlus', 'android_version': '14', 'sdk': '34'},
    {'model': 'OnePlus 12', 'brand': 'OnePlus', 'android_version': '14', 'sdk': '34'},
    {'model': 'Xiaomi 13 Pro', 'brand': 'Xiaomi', 'android_version': '13', 'sdk': '33'},
    {'model': 'Xiaomi 14', 'brand': 'Xiaomi', 'android_version': '14', 'sdk': '34'},
    {'model': 'OPPO Find X6 Pro', 'brand': 'OPPO', 'android_version': '13', 'sdk': '33'},
    {'model': 'OPPO Find X7', 'brand': 'OPPO', 'android_version': '14', 'sdk': '34'}
]

LOCALES = ['en-US', 'en-GB', 'id-ID', 'pt-BR', 'es-ES', 'fr-FR', 'de-DE', 'ja-JP', 'ko-KR', 'zh-CN', 'ru-RU']

TIMEZONES = [
    'America/New_York', 'America/Los_Angeles', 'America/Chicago', 'America/Sao_Paulo',
    'Europe/London', 'Europe/Paris', 'Europe/Berlin', 'Europe/Moscow',
    'Asia/Tokyo', 'Asia/Seoul', 'Asia/Shanghai', 'Asia/Jakarta',
    'Asia/Manila', 'Australia/Sydney'
]

PROXY_CONFIGS = [
    {'type': 'socks5', 'url': 'socks5://adit:aditganteng@47.84.66.202:1080'},
    {'type': 'socks5', 'url': 'socks5h://adit:aditganteng@43.98.166.46:1080'},
    None
]


def _mask_webhook(url: Optional[str]) -> str:
    try:
        if not url:
            return 'None'
        if '://' in url:
            scheme, rest = url.split('://', 1)
            domain = rest.split('/', 1)[0]
            return f"{scheme}://{domain}/...{url[-6:]}"
        return f"...{url[-6:]}"
    except Exception:
        return 'hidden'


def _mask_token(token: Optional[str]) -> str:
    try:
        if not token:
            return 'None'
        token = str(token)
        if len(token) <= 8:
            return f"{token[:2]}***{token[-2:]}"
        return f"{token[:4]}***{token[-4:]}"
    except Exception:
        return 'hidden'


def _mask_proxy(url: Optional[str]) -> str:
    try:
        if not url:
            return 'None'
        parsed = urlparse(url)
        host = parsed.hostname or 'unknown'
        if len(host) > 6:
            host_display = f"{host[:3]}***{host[-2:]}"
        else:
            host_display = host
        user = parsed.username or ''
        if user:
            user = f"{user[:1]}***"
        return f"{parsed.scheme}://{user + ('@' if user else '')}{host_display}:{parsed.port or ''}"
    except Exception:
        return 'hidden'


def _create_proxy_connector(proxy_config: Optional[dict]) -> Optional[ProxyConnector]:
    if not proxy_config:
        return None
    url = proxy_config.get('url')
    if not url or not isinstance(url, str):
        return None
    rdns = False
    normalized_url = url
    try:
        scheme, rest = url.split("://", 1)
        lower_scheme = scheme.lower()
        if lower_scheme in {'socks5h', 'socks4a'}:
            rdns = True
            base_scheme = lower_scheme[:-1]  # drop trailing 'h' or 'a'
            normalized_url = f"{base_scheme}://{rest}"
    except ValueError:
        lower_scheme = ''
    try:
        return ProxyConnector.from_url(normalized_url, rdns=rdns)
    except ValueError as exc:
        logger.info(
            "Proxy connector unsupported scheme url=%s error=%s",
            _mask_proxy(url),
            exc,
        )
    except Exception:
        logger.exception("Failed to create proxy connector url=%s", _mask_proxy(url))
    return None


def _create_tcp_connector() -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        limit=10,
        limit_per_host=5,
        force_close=False,
        enable_cleanup_closed=True
    )


async def _gracefully_close_session(session: Optional[aiohttp.ClientSession]):
    if not session:
        return
    connector = session.connector
    try:
        await asyncio.shield(session.close())
    except Exception:
        if connector and not connector.closed:
            with contextlib.suppress(Exception):
                await asyncio.shield(connector.close())


async def _close_connector_async(connector: Optional[aiohttp.BaseConnector]):
    if not connector:
        return
    close_fn = getattr(connector, "close", None)
    if not close_fn:
        return
    try:
        result = close_fn()
        if asyncio.iscoroutine(result):
            await result
    except Exception:
        logger.exception("Failed to close connector cleanly")


class _SharedHTTPSession:
    __slots__ = ("session", "connector", "ref_count", "lock")

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.connector: Optional[aiohttp.BaseConnector] = None
        self.ref_count: int = 0
        self.lock = asyncio.Lock()


def parse_duration_to_seconds(s: str) -> int:
    s = (s or '').strip().lower()
    if not s or len(s) < 2:
        return 0
    unit = s[-1]
    if unit not in ['s', 'm', 'h']:
        return 0
    try:
        val = int(s[:-1])
        if unit == 's':
            return val
        if unit == 'm':
            return val * 60
        if unit == 'h':
            return val * 3600
    except ValueError:
        return 0
    return 0


def format_seconds_to_duration(seconds: int) -> str:
    if seconds == 0:
        return '0s'
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def format_uptime(seconds: int) -> str:
    if seconds is None or seconds <= 0:
        return '0 Seconds'
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: List[str] = []
    if days > 0:
        parts.append(f"{int(days)} Day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{int(hours)} Hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{int(minutes)} Minute{'s' if minutes != 1 else ''}")
    if secs > 0 or not parts:
        parts.append(f"{int(secs)} Second{'s' if secs != 1 else ''}")
    return ' '.join(parts)


def _generate_nonce() -> str:
    timestamp = int(time.time() * 1000)
    random_bits = random.getrandbits(22)
    snowflake = (timestamp << 22) | random_bits
    return str(snowflake)


def _generate_device_vendor_id() -> str:
    return str(uuid.uuid4())


def _get_random_discord_config():
    version_info = random.choice(DISCORD_VERSIONS)
    device_config = random.choice(DEVICE_CONFIGS)
    locale = random.choice(LOCALES)
    timezone = random.choice(TIMEZONES)
    device_vendor_id = _generate_device_vendor_id()
    
    return {
        'version': version_info,
        'device': device_config,
        'locale': locale,
        'timezone': timezone,
        'device_vendor_id': device_vendor_id
    }


def _build_super_properties(config: dict) -> str:
    user_agent = f"Discord-Android/{config['version']['versionCode']}"
    
    props = {
        'os': 'Android',
        'browser': 'Discord Android',
        'device': config['device']['model'],
        'system_locale': config['locale'],
        'client_version': config['version']['version'],
        'release_channel': 'googleRelease',
        'device_vendor_id': config['device_vendor_id'],
        'browser_user_agent': user_agent,
        'browser_version': config['version']['version'],
        'os_version': config['device']['android_version'],
        'client_build_number': config['version']['build'],
        'client_event_source': None,
        'design_id': 0
    }
    
    json_str = json.dumps(props, separators=(',', ':'))
    encoded = base64.b64encode(json_str.encode()).decode()
    return encoded


def _get_mobile_headers(token: str, channel_id: str, guild_id: Optional[str], config: dict) -> Dict[str, str]:
    referer_guild = guild_id if guild_id else '@me'
    user_agent = f"Discord-Android/{config['version']['versionCode']}"
    
    headers = {
        'authorization': token,
        'user-agent': user_agent,
        'x-super-properties': _build_super_properties(config),
        'x-discord-locale': config['locale'],
        'x-discord-timezone': config['timezone'],
        'content-type': 'application/json',
        'accept': '*/*',
        'accept-language': f"{config['locale']},en;q=0.9",
        'accept-encoding': 'gzip, deflate, br',
        'x-debug-options': 'bugReporterEnabled',
        'origin': 'https://discord.com',
        'referer': f'https://discord.com/channels/{referer_guild}/{channel_id}',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin'
    }
    
    return headers


def _get_random_proxy_config():
    return random.choice(PROXY_CONFIGS)


class AutoPostTask:
    def __init__(
        self,
        user_id,
        account,
        channel,
        loop_count: Optional[int] = None,
        error_code: Optional[int] = None,
        error_message: Optional[str] = None,
    ):
        self.user_id = str(user_id)
        self.account = copy.deepcopy(account) if account is not None else {}
        self.channel = copy.deepcopy(channel) if channel is not None else {}
        if isinstance(self.channel, dict):
            self.channel.pop('last_error', None)
        self.token = self.account.get('token')
        self.account_user_id = str(self.account.get('user_id')) if self.account else None
        self.channel_id = str(self.channel.get('channel_id')) if self.channel else None
        self.key = f"{self.token}_{self.channel_id}" if self.token and self.channel_id else None
        self.loop_count = loop_count
        self.error_code = error_code
        self.error_message = error_message


def _describe_task(task: Optional["AutoPostTask"]) -> str:
    if not task:
        return "task=unknown"
    return (
        f"user={task.user_id} account={task.account_user_id} channel={task.channel_id} "
        f"token={_mask_token(task.token)}"
    )


class AutoPostClient:
    __slots__ = (
        'user_id', 'account', 'channel', 'token', 'channel_id', 'account_user_id', 'manager',
        '_running', '_stopped', '_task', 'start_time',
        'client_build_number', 'local_sent_count', '_last_success_time', 
        '_consecutive_failures', '_heartbeat_task',
        '_gc_task', '_fatal_handled',
        '_send_lock', '_shared_session_cache', '_shared_session_keys',
        '_default_http_session', '_default_session_lock', '_last_heartbeat',
        '_device_config', '_last_config_rotation', '_malloc_trim'
    )
    
    def __init__(self, user_id: str, account: dict, channel: dict, manager):
        self.user_id = str(user_id)
        self.manager = manager

        self.account = copy.deepcopy(account) if account is not None else {}
        self.channel = copy.deepcopy(channel) if channel is not None else {}
        if isinstance(self.channel, dict):
            self.channel.pop('last_error', None)
        self.token = (account or {}).get('token')
        self.channel_id = str((channel or {}).get('channel_id')) if channel else None
        self.account_user_id = str((account or {}).get('user_id')) if account else None

        if self.channel and self.channel_id is None:
            self.channel_id = str(self.channel.get('channel_id'))
        if self.account and self.account_user_id is None:
            self.account_user_id = str(self.account.get('user_id'))
        if self.account and self.token is None:
            self.token = self.account.get('token')

        self._running = False
        self._stopped = False
        self._task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._gc_task: Optional[asyncio.Task] = None
        self.start_time = time.monotonic()
        self.client_build_number = 450692
        self.local_sent_count = 0
        self._last_success_time = time.monotonic()
        self._consecutive_failures = 0
        self._send_lock = asyncio.Lock()
        self._shared_session_cache: Dict[str, aiohttp.ClientSession] = {}
        self._shared_session_keys: set[str] = set()
        self._default_http_session: Optional[aiohttp.ClientSession] = None
        self._default_session_lock = asyncio.Lock()
        self._last_heartbeat = time.monotonic()

        self._fatal_handled = False
        self._device_config = _get_random_discord_config()
        self._last_config_rotation = time.time()
        
        self._setup_malloc_trim()
        logger.info("AutoPostClient initialized (%s)", self._log_ctx())

    def _log_ctx(self) -> str:
        return (
            f"user={self.user_id} account={self.account_user_id} "
            f"channel={self.channel_id} token={_mask_token(self.token)}"
        )

    def _setup_malloc_trim(self):
        try:
            libc = ctypes.CDLL("libc.so.6")
            malloc_trim = libc.malloc_trim
            malloc_trim.argtypes = [ctypes.c_size_t]
            malloc_trim.restype = ctypes.c_int
            self._malloc_trim = malloc_trim
        except:
            self._malloc_trim = None
    
    async def _optimize_memory(self):
        await _run_gc_in_thread(1)
        
        if self._malloc_trim:
            try:
                await asyncio.to_thread(self._malloc_trim, 0)
            except Exception:
                pass

    def _rotate_device_config(self):
        current_time = time.time()
        rotation_interval = random.randint(1800, 7200)
        if current_time - self._last_config_rotation > rotation_interval:
            self._device_config = _get_random_discord_config()
            self._last_config_rotation = current_time

    @staticmethod
    def _is_discord_api_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
            if parsed.scheme != 'https':
                return False
            host = (parsed.hostname or '').lower()
            if host in {'discord.com', 'discordapp.com', 'canary.discord.com', 'ptb.discord.com'}:
                return (parsed.path or '').startswith('/api')
            return False
        except Exception:
            return False

    async def _get_default_http_session(self) -> aiohttp.ClientSession:
        async with self._default_session_lock:
            session = self._default_http_session
            if session and not session.closed:
                return session
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            connector = aiohttp.TCPConnector(
                limit=10,
                limit_per_host=5,
                force_close=False,
                enable_cleanup_closed=True
            )
            self._default_http_session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                trust_env=False
            )
            return self._default_http_session

    def _pick_proxy_index(self) -> int:
        if not PROXY_CONFIGS:
            return 0
        return random.randrange(len(PROXY_CONFIGS))

    async def _get_discord_api_session(self, target_url: str) -> aiohttp.ClientSession:
        if not self._is_discord_api_url(target_url):
            return await self._get_default_http_session()
        
        proxy_idx = self._pick_proxy_index()
        key_hint = f"proxy_{proxy_idx}"
        session = self._shared_session_cache.get(key_hint)
        if session and not session.closed:
            return session
        
        manager = self.manager
        if manager:
            key, session = await manager.acquire_proxy_session(proxy_idx)
            self._shared_session_cache[key] = session
            self._shared_session_keys.add(key)
            return session
        
        proxy_cfg = PROXY_CONFIGS[proxy_idx] if proxy_idx < len(PROXY_CONFIGS) else None
        connector = _create_proxy_connector(proxy_cfg) if proxy_cfg else _create_tcp_connector()
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        session = aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False)
        self._shared_session_cache[key_hint] = session
        return session

    async def _close_default_http_session(self):
        async with self._default_session_lock:
            session = self._default_http_session
            self._default_http_session = None
        if session:
            await _gracefully_close_session(session)

    async def _release_shared_sessions(self):
        if not self._shared_session_keys and not self._shared_session_cache:
            return
        manager = self.manager
        local_cache = dict(self._shared_session_cache)
        keys = list(self._shared_session_keys) or list(local_cache.keys())
        self._shared_session_keys.clear()
        self._shared_session_cache.clear()
        for key in keys:
            try:
                if manager:
                    await manager.release_proxy_session(key)
                else:
                    session = local_cache.get(key)
                    if session:
                        await _gracefully_close_session(session)
            except Exception:
                pass

    async def _close_sessions(self):
        await self._release_shared_sessions()
        await self._close_default_http_session()

    async def _reset_session(self):
        logger.info("AutoPostClient resetting sessions (%s)", self._log_ctx())
        await self._release_shared_sessions()
        await self._close_default_http_session()
        await asyncio.sleep(0.5)

    def _reset_runtime_state(self):
        now = time.monotonic()
        self.start_time = now
        self.local_sent_count = 0
        self._last_success_time = now
        self._consecutive_failures = 0
        self._last_heartbeat = now
        self._fatal_handled = False
        self._device_config = _get_random_discord_config()
        self._last_config_rotation = time.time()

    async def _cancel_task(self, task: Optional[asyncio.Task], timeout: float = 3.0):
        if not task or task.done():
            return
        
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except asyncio.CancelledError:
            pass
        except asyncio.TimeoutError:
            pass
        except Exception:
            logger.exception("AutoPostManager webhook notification error (%s)", _describe_task(task))

    async def _cancel_background_tasks(self):
        tasks = []
        
        if self._heartbeat_task:
            tasks.append(self._cancel_task(self._heartbeat_task, 2.0))
            self._heartbeat_task = None
        
        if self._gc_task:
            tasks.append(self._cancel_task(self._gc_task, 2.0))
            self._gc_task = None
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def start(self):
        if self._running or self._stopped:
            logger.info(
                "AutoPostClient.start ignored (running=%s stopped=%s) (%s)",
                self._running,
                self._stopped,
                self._log_ctx(),
            )
            return

        logger.info("AutoPostClient.start requested (%s)", self._log_ctx())

        await self._cancel_background_tasks()
        await self._close_sessions()
        await self._refresh_channel_config()
        
        if not self.token or not self.channel_id:
            logger.info(
                "AutoPostClient.start aborted missing token/channel (%s)",
                self._log_ctx(),
            )
            return

        self._reset_runtime_state()
        logger.info(
            "AutoPostClient runtime state reset base_delay=%s random_delay=%s (%s)",
            self.channel.get('delay'),
            self.channel.get('delay_random'),
            self._log_ctx(),
        )
        
        self._running = True
        self._stopped = False
        
        self._task = asyncio.create_task(self._posting_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
        self._gc_task = asyncio.create_task(self._periodic_gc())
        logger.info("AutoPostClient tasks scheduled (%s)", self._log_ctx())

    async def _heartbeat_monitor(self):
        try:
            while self._running and not self._stopped:
                try:
                    await asyncio.sleep(120)
                    
                    if not self._running or self._stopped:
                        break
                    
                    time_since_success = time.monotonic() - self._last_success_time
                    base_delay = parse_duration_to_seconds(self.channel.get('delay', '2m')) or 120
                    
                    max_expected_delay = base_delay * 4
                    
                    if time_since_success > max_expected_delay and time_since_success > 600:
                        try:
                            logger.info(
                                "Heartbeat detected stalled loop idle=%.2fs (max_expected=%.2fs) (%s)",
                                time_since_success,
                                max_expected_delay,
                                self._log_ctx(),
                            )
                            await self._reset_session()
                            self._consecutive_failures = 0
                        except Exception:
                            pass
                    
                    self._last_heartbeat = time.monotonic()
                    
                except asyncio.CancelledError:
                    break
                except Exception:
                    if not self._running or self._stopped:
                        break
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def stop(self):
        if self._stopped:
            logger.info("AutoPostClient.stop ignored (already stopped) (%s)", self._log_ctx())
            return
        logger.info("AutoPostClient.stop requested (%s)", self._log_ctx())
        
        self._stopped = True
        self._running = False
        
        await self._cancel_background_tasks()
        
        if self._task and not self._task.done():
            await self._cancel_task(self._task, 5.0)
        
        self._task = None
        
        await self._close_sessions()
        
        await self._optimize_memory()
        logger.info("AutoPostClient.stop completed (%s)", self._log_ctx())

    def _cancel_all_tasks_nowait(self):
        tasks = [self._task, self._heartbeat_task, self._gc_task]
        self._task = None
        self._heartbeat_task = None
        self._gc_task = None
        for task in tasks:
            if task and not task.done():
                task.cancel()

    async def force_close(self):
        logger.info("AutoPostClient.force_close invoked (%s)", self._log_ctx())
        self._running = False
        self._stopped = True
        self._cancel_all_tasks_nowait()
        try:
            await self._close_sessions()
        except Exception:
            pass
        logger.info("AutoPostClient.force_close completed (%s)", self._log_ctx())
        try:
            await self._optimize_memory()
        except Exception:
            pass

    async def _posting_loop(self):
        loop = asyncio.get_running_loop()
        next_time = loop.time()
        
        try:
            while self._running and not self._stopped:
                try:
                    now = loop.time()
                    if now < next_time:
                        sleep_time = max(0.1, min(next_time - now, 300))
                        await asyncio.sleep(sleep_time)
                        continue

                    if not self._running or self._stopped:
                        break

                    try:
                        gb_until = self.manager.get_token_global_backoff(self.token)
                    except Exception:
                        gb_until = 0.0
                    
                    if gb_until and now < gb_until:
                        next_time = gb_until
                        continue
                    
                    result, retry_after = await self._send_post()
                    
                    if not self._running or self._stopped:
                        break
                    
                    if result == 'ok':
                        self._last_success_time = time.monotonic()
                        self._last_heartbeat = time.monotonic()
                        self._consecutive_failures = 0
                        base_delay = parse_duration_to_seconds(self.channel.get('delay', '2m')) or 120
                        final_delay = base_delay
                        rd = (self.channel.get('delay_random') or '').strip()
                        if rd and rd.lower() != 'none':
                            try:
                                parts = [parse_duration_to_seconds(p) for p in rd.split(',') if p.strip()]
                                parts = [p for p in parts if p > 0]
                                if parts:
                                    chosen = random.choice(parts)
                                    final_delay += chosen
                            except Exception:
                                pass
                        delay_applied = max(1, final_delay)
                        next_time = loop.time() + delay_applied
                        continue

                    elif result == 'rate_limited':
                        self._consecutive_failures = 0
                        delay_applied = max(1.0, float(retry_after or 1.0))
                        next_time = loop.time() + delay_applied
                        continue

                    elif result == 'fatal':
                        logger.info("AutoPostClient posting loop received fatal result (%s)", self._log_ctx())
                        break
                    
                    else:
                        self._consecutive_failures += 1
                        logger.info(
                            "AutoPostClient posting loop failure count=%s (%s)",
                            self._consecutive_failures,
                            self._log_ctx(),
                        )
                        
                        if self._consecutive_failures >= 5:
                            try:
                                logger.info(
                                    "AutoPostClient consecutive failure threshold reached; resetting sessions (%s)",
                                    self._log_ctx(),
                                )
                                await self._reset_session()
                                self._consecutive_failures = 0
                            except Exception:
                                await asyncio.sleep(60)
                                continue
                                
                        base_delay = parse_duration_to_seconds(self.channel.get('delay', '2m')) or 120
                        delay_applied = max(30, base_delay)
                        next_time = loop.time() + delay_applied
                
                except asyncio.CancelledError:
                    break
                except Exception:
                    if not self._running or self._stopped:
                        break
                    self._consecutive_failures += 1
                    logger.exception(
                        "AutoPostClient unexpected error inside posting loop (failure_count=%s) (%s)",
                        self._consecutive_failures,
                        self._log_ctx(),
                    )
                    await asyncio.sleep(30)
        
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            self._running = False
            self._stopped = True

    async def _periodic_gc(self):
        try:
            while self._running and not self._stopped:
                try:
                    await asyncio.sleep(60)
                    
                    if not self._running or self._stopped:
                        break
                    
                    await self._optimize_memory()
                    
                except asyncio.CancelledError:
                    break
                except Exception:
                    if not self._running or self._stopped:
                        break
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _refresh_channel_config(self):
        try:
            snapshot = await self.manager.run_db_task(self._fetch_channel_config_snapshot)
        except Exception:
            snapshot = None
        if not snapshot:
            logger.info("AutoPostClient channel snapshot unavailable (%s)", self._log_ctx())
            return
        account_snapshot, channel_snapshot = snapshot
        try:
            self.account = account_snapshot
            self.channel = channel_snapshot
            if isinstance(self.channel, dict):
                self.channel.pop('last_error', None)
            if self.account and self.account_user_id is None:
                self.account_user_id = str(self.account.get('user_id'))
            if self.account and self.token is None:
                self.token = self.account.get('token')
            if self.channel and self.channel_id is None:
                self.channel_id = str(self.channel.get('channel_id'))
        except Exception:
            pass

    def _fetch_channel_config_snapshot(self):
        try:
            db = get_db()
            user_config = db.get_user(self.user_id)
            if not user_config:
                return None
            account = next(
                (acc for acc in user_config.get('accounts', []) if str(acc.get('user_id')) == str(self.account_user_id)),
                None,
            )
            if not account:
                return None
            channel = next(
                (ch for ch in account.get('channels', []) if str(ch.get('channel_id')) == str(self.channel_id)),
                None,
            )
            if not channel:
                return None
            return copy.deepcopy(account), copy.deepcopy(channel)
        except Exception:
            return None

    async def _send_post(self) -> Tuple[str, float]:
        if self._stopped or not self._running:
            return 'error', 0.0
        
        await self._refresh_channel_config()
        content = self.channel.get('teks') or ''
        logger.info(
            "AutoPostClient attempting send content_length=%s (%s)",
            len(content),
            self._log_ctx(),
        )
        
        self._rotate_device_config()

        headers = _get_mobile_headers(
            self.token, 
            self.channel_id, 
            self.channel.get('guild_id'),
            self._device_config
        )
        
        url = f'https://discord.com/api/v10/channels/{self.channel_id}/messages'

        result: str = 'error'
        retry_after: float = 0.0
        
        session = await self._get_discord_api_session(url)

        async with self._send_lock:
            try:
                nonce = _generate_nonce()
                payload = {
                    'content': content,
                    'nonce': nonce,
                    'tts': False,
                    'flags': 0
                }
                
                await asyncio.sleep(random.uniform(0.2, 0.8))
                
                async with session.post(url, headers=headers, json=payload) as resp:
                    status = resp.status
                    if status in (200, 201, 204):
                        result = 'ok'
                        logger.info(
                            "AutoPostClient send success status=%s (%s)",
                            status,
                            self._log_ctx(),
                        )
                        await self._handle_successful_send()
                    elif status == 429:
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            data = None
                        retry_after, is_global = self._parse_rate_limit(data, resp.headers)
                        if is_global:
                            try:
                                self.manager.set_token_global_backoff(self.token, asyncio.get_running_loop().time() + max(0.0, retry_after))
                            except Exception:
                                pass
                        logger.info(
                            "AutoPostClient rate limited retry_after=%.2fs global=%s (%s)",
                            retry_after,
                            is_global,
                            self._log_ctx(),
                        )
                        result = 'rate_limited'
                    elif status == 401:
                        logger.info("AutoPostClient token expired response received (%s)", self._log_ctx())
                        await self._handle_token_expired()
                        result = 'fatal'
                    elif status == 403:
                        logger.info("AutoPostClient permission denied response received (%s)", self._log_ctx())
                        await self._handle_permission_denied()
                        result = 'fatal'
                    elif status >= 500:
                        logger.info(
                            "AutoPostClient Discord server error status=%s (%s)",
                            status,
                            self._log_ctx(),
                        )
                        result = 'error'
                    else:
                        try:
                            data = await resp.json(content_type=None)
                        except:
                            data = None
                        try:
                            text = await resp.text()
                        except:
                            text = ''
                        await self._handle_unexpected_error(status, data, text)
                        result = 'error'
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("AutoPostClient send_post encountered exception (%s)", self._log_ctx())
                result = 'error'

        return result, retry_after

    async def _handle_successful_send(self):
        self.local_sent_count += 1
        logger.info(
            "AutoPostClient successful send recorded total=%s (%s)",
            self.local_sent_count,
            self._log_ctx(),
        )
        try:
            account_user_id = (self.account or {}).get('user_id')
            if account_user_id and self.channel_id:
                await self.manager.record_sent_count(self.user_id, account_user_id, self.channel_id)
        except Exception:
            pass
        
        try:
            task = AutoPostTask(self.user_id, self.account, self.channel, self.local_sent_count)
            await self.manager._send_webhook_notification(task)
        except Exception:
            pass

    def _parse_rate_limit(self, data: Optional[dict], headers) -> Tuple[float, bool]:
        is_global = False
        retry_after = 1.0
        
        if data:
            retry_after = float(data.get('retry_after', 1.0))
            is_global = data.get('global', False)
        else:
            retry_header = headers.get('Retry-After') or headers.get('retry-after')
            if retry_header:
                try:
                    retry_after = float(retry_header)
                except:
                    retry_after = 1.0
            
            global_header = headers.get('X-RateLimit-Global') or headers.get('x-ratelimit-global')
            if global_header:
                is_global = str(global_header).lower() == 'true'
        
        return retry_after, is_global

    async def _handle_token_expired(self):
        if self._fatal_handled:
            return
        self._fatal_handled = True
        logger.info("AutoPostClient handling token expired (%s)", self._log_ctx())
        
        try:
            await self.manager.handle_token_expired(AutoPostTask(self.user_id, self.account, self.channel))
        except Exception:
            pass

    async def _handle_permission_denied(self):
        if self._fatal_handled:
            return
        self._fatal_handled = True
        logger.info("AutoPostClient handling permission denied (%s)", self._log_ctx())
        
        try:
            await self.manager.handle_permission_issue(AutoPostTask(self.user_id, self.account, self.channel))
        except Exception:
            pass

    async def _handle_unexpected_error(self, status_code: int, data: Optional[dict], text: str):
        if self._fatal_handled:
            return
        
        try:
            logger.info(
                "AutoPostClient handling unexpected error status=%s (%s)",
                status_code,
                self._log_ctx(),
            )
            if status_code >= 500:
                return
            
            if status_code in [400, 404, 405]:
                self._fatal_handled = True
                detail = None
                if isinstance(data, dict):
                    detail = data.get('message') or data.get('code')
                    if not detail:
                        try:
                            detail = json.dumps(data)
                        except Exception:
                            detail = str(data)
                if not detail:
                    detail = (text or '').strip()
                if detail and len(detail) > 200:
                    detail = f"{detail[:197]}..."
                task = AutoPostTask(
                    self.user_id,
                    self.account,
                    self.channel,
                    error_code=status_code,
                    error_message=detail or "Unexpected error from Discord API",
                )
                await self.manager.handle_unexpected_error(task)
        except Exception:
            pass


class AutoPostManager:
    def __init__(self, config, worker_count: int = 1):
        self.config = config
        self.clients: Dict[str, AutoPostClient] = {}
        self._lock = asyncio.Lock()
        self._token_global_backoff: Dict[str, float] = {}
        self._executor: Optional[ThreadPoolExecutor] = None
        self._thread_local = threading.local()
        self._thread_lock = threading.Lock()
        self._wh_session: Optional[aiohttp.ClientSession] = None
        self._session_pool: Dict[str, _SharedHTTPSession] = {}
        self._session_pool_lock = asyncio.Lock()
        self._running = False
        self._periodic_gc_task: Optional[asyncio.Task] = None
        self._inspect_task: Optional[asyncio.Task] = None
        self._sent_flush_task: Optional[asyncio.Task] = None
        try:
            self._worker_count = max(1, int(worker_count))
        except Exception:
            self._worker_count = 1
        try:
            self._loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None
        self._sent_count_buffer: Dict[Tuple[str, str, str], int] = {}
        self._sent_buffer_lock = asyncio.Lock()
        self._sent_flush_interval = 15.0
        self._sent_flush_threshold = 25
        logger.info("AutoPostManager initialized worker_count=%s", self._worker_count)

    def _ensure_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = None
        return self._loop

    async def _run_on_manager_loop(self, coro: Awaitable):
        loop = self._ensure_loop()
        if loop is None:
            return await coro

        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if current_loop is loop:
            return await coro

        future = asyncio.run_coroutine_threadsafe(coro, loop)
        if current_loop is None:
            return await asyncio.to_thread(future.result)
        return await asyncio.wrap_future(future)

    def _choose_global_webhook(self) -> Optional[str]:
        value = self.config.get('urlwebhook')
        if isinstance(value, str):
            value = value.strip()
            return value or None
        if isinstance(value, (list, tuple, set)):
            candidates = [
                str(item).strip()
                for item in value
                if isinstance(item, str) and item.strip()
            ]
            if candidates:
                return random.choice(candidates)
        return None

    async def _resolve_webhook_target(self, user_id: str, account: Optional[dict]) -> Tuple[Optional[str], Optional[dict]]:
        def fetch_user():
            db = get_db()
            return db.get_user(user_id)

        try:
            db_user = await self.run_db_task(fetch_user)
        except Exception:
            db_user = None

        def normalize(value: Optional[str]) -> Optional[str]:
            if not value:
                return None
            value = str(value).strip()
            return value or None

        account_webhook = normalize((account or {}).get('webhookurl') if account else None)
        user_webhook = normalize(db_user.get('webhookurl')) if isinstance(db_user, dict) else None
        target = account_webhook or user_webhook or self._choose_global_webhook()
        return target, db_user

    @staticmethod
    def _session_key_for_proxy(proxy_idx: int) -> str:
        return f"proxy_{proxy_idx}"

    def _proxy_config_for_index(self, proxy_idx: int) -> Optional[dict]:
        if 0 <= proxy_idx < len(PROXY_CONFIGS):
            return PROXY_CONFIGS[proxy_idx]
        return None

    async def acquire_proxy_session(self, proxy_idx: int) -> tuple[str, aiohttp.ClientSession]:
        key = self._session_key_for_proxy(proxy_idx)
        async with self._session_pool_lock:
            shared = self._session_pool.get(key)
            if shared is None:
                shared = _SharedHTTPSession()
                self._session_pool[key] = shared
        async with shared.lock:
            if not shared.session or shared.session.closed:
                proxy_cfg = self._proxy_config_for_index(proxy_idx)
                connector = _create_proxy_connector(proxy_cfg) if proxy_cfg else _create_tcp_connector()
                shared.connector = connector
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                shared.session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                    trust_env=False
                )
            shared.ref_count += 1
            return key, shared.session

    async def release_proxy_session(self, key: str):
        async with self._session_pool_lock:
            shared = self._session_pool.get(key)
        if not shared:
            return
        cleanup = False
        session = None
        connector = None
        async with shared.lock:
            if shared.ref_count > 0:
                shared.ref_count -= 1
            if shared.ref_count == 0:
                cleanup = True
                session = shared.session
                connector = shared.connector
                shared.session = None
                shared.connector = None
        if cleanup:
            await _gracefully_close_session(session)
            if connector:
                await _close_connector_async(connector)
            async with self._session_pool_lock:
                self._session_pool.pop(key, None)

    def start(self):
        if self._running:
            logger.info("AutoPostManager.start ignored (already running)")
            return
        logger.info("AutoPostManager.start invoked")
        self._running = True
        
        if not self._periodic_gc_task or self._periodic_gc_task.done():
            self._periodic_gc_task = asyncio.create_task(self._periodic_gc())
        
        if not self._inspect_task or self._inspect_task.done():
            self._inspect_task = asyncio.create_task(self._inspect_clients())
        
        if not self._sent_flush_task or self._sent_flush_task.done():
            self._sent_flush_task = asyncio.create_task(self._sent_flush_loop())

    def stop(self):
        if not self._running:
            logger.info("AutoPostManager.stop ignored (not running)")
            return
        self._running = False
        logger.info("AutoPostManager.stop invoked")
        asyncio.create_task(self.stop_and_wait())

    async def stop_and_wait(self):
        logger.info("AutoPostManager.stop_and_wait started")
        self._running = False
        
        if self._periodic_gc_task and not self._periodic_gc_task.done():
            self._periodic_gc_task.cancel()
            try:
                await self._periodic_gc_task
            except asyncio.CancelledError:
                pass
        
        if self._inspect_task and not self._inspect_task.done():
            self._inspect_task.cancel()
            try:
                await self._inspect_task
            except asyncio.CancelledError:
                pass

        if self._sent_flush_task and not self._sent_flush_task.done():
            self._sent_flush_task.cancel()
            try:
                await self._sent_flush_task
            except asyncio.CancelledError:
                pass
        self._sent_flush_task = None

        await self._flush_sent_counts()
        await self._finalize_stop(wait_for_executor=True)
        logger.info("AutoPostManager.stop_and_wait completed")

    async def shutdown(self):
        try:
            await self.stop_and_wait()
        except Exception:
            pass
    
    async def stop_all(self):
        await self.stop_and_wait()

    async def _periodic_gc(self):
        while self._running:
            try:
                await asyncio.sleep(60)
                if not self._running:
                    break
                
                await _run_gc_in_thread()
                
                current_time = asyncio.get_running_loop().time()
                expired_tokens = [
                    token for token, backoff_until in list(self._token_global_backoff.items())
                    if current_time > backoff_until
                ]
                for token in expired_tokens:
                    self._token_global_backoff.pop(token, None)
                
                async with self._lock:
                    inactive_keys = []
                    for key, client in self.clients.items():
                        if not client._running or client._stopped:
                            inactive_keys.append(key)
                
                for key in inactive_keys:
                    try:
                        await self._remove_task_async(key)
                    except:
                        pass
                
            except asyncio.CancelledError:
                break
            except:
                pass

    async def _inspect_clients(self):
        while self._running:
            try:
                await asyncio.sleep(600)
                if not self._running:
                    break
                
                stale_keys = []
                async with self._lock:
                    for key, client in list(self.clients.items()):
                        if not client._running:
                            continue
                        time_since_heartbeat = time.monotonic() - client._last_heartbeat
                        if time_since_heartbeat > 1200:
                            stale_keys.append(key)
                
                for key in stale_keys:
                    try:
                        await self._restart_task(key)
                    except Exception:
                        pass
                        
            except asyncio.CancelledError:
                break
            except:
                pass

    async def _restart_task(self, key: str):
        async with self._lock:
            client = self.clients.get(key)
            if not client:
                logger.info("AutoPostManager.restart skipped; client missing key=%s", key)
                return
            
            user_id = client.user_id
            account = copy.deepcopy(client.account) if client.account else {}
            channel = copy.deepcopy(client.channel) if client.channel else {}
        
        logger.info(
            "AutoPostManager.restart initiated key=%s user=%s channel=%s",
            key,
            user_id,
            channel.get('channel_id') if isinstance(channel, dict) else None,
        )
        
        try:
            await self.remove_task(key)
        except Exception:
            pass
        
        await asyncio.sleep(1.0)
        
        def fetch_user():
            db = get_db()
            return db.get_user(user_id)

        user = await self.run_db_task(fetch_user)
        if not user:
            logger.info("AutoPostManager.restart aborted; user not found key=%s", key)
            return
        
        for acc in user.get('accounts', []):
            if acc['user_id'] == account['user_id']:
                for ch in acc.get('channels', []):
                    if ch['channel_id'] == channel['channel_id'] and ch.get('is_active'):
                        task = AutoPostTask(user_id, acc, ch)
                        await self.add_task(task)
                        logger.info(
                            "AutoPostManager.restart re-added task key=%s channel=%s",
                            key,
                            ch.get('channel_id'),
                        )
                        return
        logger.info("AutoPostManager.restart finished without re-adding key=%s", key)

    async def _stop_all(self):
        async with self._lock:
            clients_list = list(self.clients.values())
            self.clients.clear()
        logger.info("AutoPostManager._stop_all invoked client_count=%s", len(clients_list))
        
        stop_tasks = []
        for client in clients_list:
            stop_tasks.append(self._safe_stop_client(client))
        
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)
        logger.info("AutoPostManager._stop_all finished stopping clients")
        
        await self._close_webhook_session()
        await self._close_all_shared_sessions()
        await self._flush_sent_counts()
        
        await _run_gc_in_thread()

    async def _close_webhook_session(self):
        session = self._wh_session
        self._wh_session = None
        if not session:
            return
        logger.info("AutoPostManager closing webhook session")
        await _gracefully_close_session(session)
        await asyncio.sleep(0.1)

    async def _close_all_shared_sessions(self):
        async with self._session_pool_lock:
            entries = list(self._session_pool.items())
            self._session_pool.clear()
        if not entries:
            return
        close_tasks = []
        connector_tasks = []
        for key, shared in entries:
            async with shared.lock:
                session = shared.session
                connector = shared.connector
                shared.session = None
                shared.connector = None
                shared.ref_count = 0
            if session:
                close_tasks.append(asyncio.create_task(_gracefully_close_session(session)))
            if connector:
                connector_tasks.append(asyncio.create_task(_close_connector_async(connector)))
        pending = close_tasks + connector_tasks
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
            await asyncio.sleep(0.1)

    async def _safe_stop_client(self, client: AutoPostClient):
        logger.info("AutoPostManager._safe_stop_client invoked (%s)", client._log_ctx())
        try:
            await asyncio.wait_for(client.stop(), timeout=8.0)
        except asyncio.TimeoutError:
            try:
                await client.force_close()
            except Exception:
                pass
        except Exception:
            try:
                await client.force_close()
            except Exception:
                pass
        logger.info("AutoPostManager._safe_stop_client finished (%s)", client._log_ctx())

    def _create_executor(self) -> ThreadPoolExecutor:
        workers = self._worker_count or 1
        try:
            return ThreadPoolExecutor(max_workers=workers, thread_name_prefix="autopost")
        except Exception:
            return ThreadPoolExecutor(max_workers=workers)

    def _ensure_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = self._create_executor()
            self._thread_local = threading.local()
        return self._executor

    def _thread_gc_guard(self):
        last_gc = getattr(self._thread_local, 'last_gc', 0.0)
        now = time.monotonic()
        if now - last_gc >= 60.0:
            gc.collect(generation=0)
            self._thread_local.last_gc = now

    async def _shutdown_executor(self, wait: bool):
        executor = self._executor
        if not executor:
            return
        self._executor = None

        loop = asyncio.get_running_loop()

        def _shutdown():
            try:
                executor.shutdown(wait=wait, cancel_futures=True)
            except TypeError:
                executor.shutdown(wait=wait)

        await loop.run_in_executor(None, _shutdown)
        self._thread_local = threading.local()

    async def _finalize_stop(self, wait_for_executor: bool):
        try:
            await self._stop_all()
        finally:
            await self._shutdown_executor(wait=wait_for_executor)

    async def run_in_thread(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        executor = self._ensure_executor()
        bound = partial(func, *args, **kwargs)

        def wrapper():
            self._thread_gc_guard()
            return bound()

        return await loop.run_in_executor(executor, wrapper)

    async def run_db_task(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        executor = self._ensure_executor()

        def locked_call():
            self._thread_gc_guard()
            with self._thread_lock:
                return func(*args, **kwargs)

        return await loop.run_in_executor(executor, locked_call)

    async def record_sent_count(self, user_id, account_user_id, channel_id, amount: int = 1):
        if not user_id or not account_user_id or not channel_id:
            return
        try:
            amount = int(amount)
        except Exception:
            amount = 1
        if amount <= 0:
            amount = 1
        key = (str(user_id), str(account_user_id), str(channel_id))
        immediate_batch = None
        async with self._sent_buffer_lock:
            current = self._sent_count_buffer.get(key, 0) + amount
            self._sent_count_buffer[key] = current
            if not self._running or current >= self._sent_flush_threshold:
                pending = self._sent_count_buffer.pop(key, 0)
                if pending > 0:
                    immediate_batch = {key: pending}
        if immediate_batch:
            await self._flush_sent_counts(immediate_batch)

    async def _sent_flush_loop(self):
        while self._running:
            try:
                await asyncio.sleep(self._sent_flush_interval)
                await self._flush_sent_counts()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("AutoPostManager sent-count flush loop error")
        await self._flush_sent_counts()

    async def _flush_sent_counts(self, batch: Optional[Dict[Tuple[str, str, str], int]] = None):
        if batch is None:
            async with self._sent_buffer_lock:
                if not self._sent_count_buffer:
                    return
                batch = self._sent_count_buffer
                self._sent_count_buffer = {}
        if not batch:
            return

        flush_batch = {key: val for key, val in batch.items() if val}
        if not flush_batch:
            return

        def _write(payload):
            db = get_db()
            for (user_id, account_user_id, channel_id), amount in payload.items():
                try:
                    if hasattr(db, "increment_sent_count_by"):
                        db.increment_sent_count_by(user_id, account_user_id, channel_id, amount)
                    else:
                        for _ in range(amount):
                            db.increment_sent_count(user_id, account_user_id, channel_id)
                except Exception:
                    logger.exception(
                        "AutoPostManager failed to persist sent_count user=%s account=%s channel=%s amount=%s",
                        user_id,
                        account_user_id,
                        channel_id,
                        amount,
                    )

        await self.run_db_task(_write, flush_batch)

    def get_token_global_backoff(self, token: str) -> float:
        try:
            return float(self._token_global_backoff.get(token) or 0.0)
        except Exception:
            return 0.0

    def set_token_global_backoff(self, token: str, until_monotonic: float):
        try:
            cur = float(self._token_global_backoff.get(token) or 0.0)
            if until_monotonic > cur:
                self._token_global_backoff[token] = until_monotonic
        except Exception:
            self._token_global_backoff[token] = until_monotonic

    async def load_active_channels_from_db(self):
        return []

    async def add_task(self, task: AutoPostTask):
        logger.info("AutoPostManager.add_task requested (%s)", _describe_task(task))
        await self._run_on_manager_loop(self._add_task_async(task))

    async def _add_task_async(self, task: AutoPostTask):
        key = task.key
        if not key:
            return
        
        async with self._lock:
            if key in self.clients:
                logger.info(
                    "AutoPostManager.add_task skipped existing client key=%s (%s)",
                    key,
                    _describe_task(task),
                )
                return
            client = AutoPostClient(task.user_id, task.account, task.channel, self)
            self.clients[key] = client
        
        try:
            await client.start()
            logger.info("AutoPostManager client started key=%s (%s)", key, _describe_task(task))
        except Exception:
            async with self._lock:
                self.clients.pop(key, None)
            try:
                await client.force_close()
            except Exception:
                pass
            logger.exception("AutoPostManager failed to start client key=%s (%s)", key, _describe_task(task))

    async def remove_task(self, key: str):
        logger.info("AutoPostManager.remove_task requested key=%s", key)
        await self._run_on_manager_loop(self._remove_task_async(key))

    async def _remove_task_async(self, key: str):
        async with self._lock:
            client = self.clients.pop(key, None)
        
        if client:
            try:
                await client.stop()
            except Exception:
                try:
                    await client.force_close()
                except Exception:
                    pass
            finally:
                await _run_gc_in_thread()
        logger.info("AutoPostManager.remove_task completed key=%s client_exists=%s", key, bool(client))

    def get_task(self, key: str):
        return self.clients.get(key)

    async def _send_error_notification(self, user_id: str, account: dict, error_type: str, channel_info: dict, error_code: int = None, error_message: str = None):
        try:
            logger.info(
                "AutoPostManager webhook error notification type=%s target_user=%s account=%s channel=%s",
                error_type,
                user_id,
                (account or {}).get('user_id'),
                (channel_info or {}).get('channel_id'),
            )
            target_webhook_url, _ = await self._resolve_webhook_target(user_id, account)
            if not target_webhook_url:
                logger.info(
                    "AutoPostManager error notification skipped (no webhook) user=%s",
                    user_id,
                )
                return

            embed = self._build_error_embed(account, channel_info, error_type, error_code, error_message)
            footer_text = self.config.get('global_footer')
            if footer_text:
                embed.setdefault('footer', {})
                embed['footer']['text'] = footer_text
                embed['footer']['icon_url'] = self.config.get('global_footer_icon', '')

            session = self._get_webhook_session()
            async with session.post(target_webhook_url, json={"embeds": [embed]}) as resp:
                logger.info(
                    "AutoPostManager error webhook delivered status=%s user=%s type=%s",
                    resp.status,
                    user_id,
                    error_type,
                )
        except Exception:
            pass

    def _build_error_embed(
        self,
        account: dict,
        channel_info: dict,
        error_type: str,
        error_code: Optional[int],
        error_message: Optional[str],
    ) -> dict:
        timestamp_utc = datetime.now(timezone.utc).isoformat()
        account_name = (account or {}).get('username', 'Unknown')
        account_id = (account or {}).get('user_id', 'N/A')
        channel_data = channel_info or {}
        channel_id = channel_data.get('channel_id', 'N/A')
        server_name = channel_data.get('server_name', 'N/A')

        def account_field():
            return {
                "name": "Account Information",
                "value": (
                    f"**<:dot:1426148484146270269> Username**: {account_name}\n"
                    f"**<:dot:1426148484146270269> User ID**: {account_id}"
                ),
                "inline": False
            }

        if error_type == 'token_expired':
            embed = {
                "title": "<:exclamation:1426164277638594680> Token Expired",
                "description": f"The token for your account **{account_name}** has expired or is invalid.",
                "color": 0xFF0000,
                "fields": [
                    account_field(),
                    {
                        "name": "Action Required",
                        "value": "Please update your token using the `/account` command. All autopost tasks for this account have been stopped.",
                        "inline": False
                    },
                ],
                "timestamp": timestamp_utc,
            }
            return embed

        if error_type == 'permission_denied':
            embed = {
                "title": "<:exclamation:1426164277638594680> Permission Denied",
                "description": f"Your account **{account_name}** cannot send messages to the target channel.",
                "color": 0xFF6B00,
                "fields": [
                    account_field(),
                    {
                        "name": "Channel Information",
                        "value": (
                            f"**<:dot:1426148484146270269> Channel ID**: {channel_id}\n"
                            f"**<:dot:1426148484146270269> Server**: {server_name}"
                        ),
                        "inline": False
                    },
                    {
                        "name": "Possible Causes",
                        "value": "• Missing Send Messages permission\n• Channel was deleted\n• Account was banned/kicked from server",
                        "inline": False
                    },
                ],
                "timestamp": timestamp_utc,
            }
            return embed

        if error_type == 'unexpected_error':
            fields = [
                account_field(),
                {
                    "name": "Channel Information",
                    "value": (
                        f"**<:dot:1426148484146270269> Channel ID**: {channel_id}\n"
                        f"**<:dot:1426148484146270269> Server**: {server_name}"
                    ),
                    "inline": False
                },
            ]
            detail_lines = []
            if error_code is not None:
                detail_lines.append(f"**<:dot:1426148484146270269> Status Code**: {error_code}")
            if error_message:
                detail_lines.append(f"**<:dot:1426148484146270269> Detail**: {error_message}")
            if detail_lines:
                fields.append(
                    {
                        "name": "Error Detail",
                        "value": "\n".join(detail_lines),
                        "inline": False
                    }
                )
            fields.append(
                {
                    "name": "Action Required",
                    "value": "Please double-check the channel configuration or recreate the autopost task.",
                    "inline": False
                }
            )
            return {
                "title": "<:exclamation:1426164277638594680> Unexpected Autopost Error",
                "description": "Autopost task stopped because Discord returned an unexpected response.",
                "color": 0xFF6600,
                "fields": fields,
                "timestamp": timestamp_utc,
            }

        return {
            "title": "<:exclamation:1426164277638594680> Autopost Error",
            "description": f"An error occurred with account **{account_name}**",
            "color": 0xFF0000,
            "fields": [account_field()],
            "timestamp": timestamp_utc,
        }

    async def handle_token_expired(self, task: AutoPostTask):
        try:
            logger.info("AutoPostManager.handle_token_expired invoked (%s)", _describe_task(task))
            if task.key:
                await self._remove_task_async(task.key)
            
            def db_task():
                db = get_db()
                user = db.get_user(task.user_id)
                if not user:
                    return False
                for acc in user.get('accounts', []):
                    if str(acc.get('user_id')) == str(task.account_user_id):
                        acc['is_active'] = False
                        for ch in acc.get('channels', []):
                            ch['is_active'] = False
                        break
                db.add_user(user)
                return True

            await self.run_db_task(db_task)
            await self._send_error_notification(task.user_id, task.account, 'token_expired', task.channel)
            
        except Exception:
            pass

    async def handle_permission_issue(self, task: AutoPostTask):
        try:
            logger.info("AutoPostManager.handle_permission_issue invoked (%s)", _describe_task(task))
            if task.key:
                await self._remove_task_async(task.key)
        
            def db_task():
                db = get_db()
                user = db.get_user(task.user_id)
                if not user:
                    return False
                for acc in user.get('accounts', []):
                    if str(acc.get('user_id')) != str(task.account_user_id):
                        continue
                    for ch in acc.get('channels', []):
                        if str(ch.get('channel_id')) == str(task.channel_id):
                            ch['is_active'] = False
                            ch['last_error'] = 'permission_denied'
                            break
                    break
                db.add_user(user)
                return True

            await self.run_db_task(db_task)
            await self._send_error_notification(task.user_id, task.account, 'permission_denied', task.channel)
            
        except Exception:
            pass

    async def handle_unexpected_error(self, task: AutoPostTask):
        try:
            logger.info("AutoPostManager.handle_unexpected_error invoked (%s)", _describe_task(task))
            if task.key:
                await self._remove_task_async(task.key)
            
            def db_task():
                db = get_db()
                user = db.get_user(task.user_id)
                if not user:
                    return False
                for acc in user.get('accounts', []):
                    if str(acc.get('user_id')) != str(task.account_user_id):
                        continue
                    for ch in acc.get('channels', []):
                        if str(ch.get('channel_id')) == str(task.channel_id):
                            ch['is_active'] = False
                            ch['last_error'] = f'unexpected_error_{time.time():.0f}'
                            break
                    break
                db.add_user(user)
                return True

            await self.run_db_task(db_task)
            await self._send_error_notification(
                task.user_id,
                task.account,
                'unexpected_error',
                task.channel,
                error_code=task.error_code,
                error_message=task.error_message,
            )
            
        except Exception:
            pass

    def _get_webhook_session(self) -> aiohttp.ClientSession:
        if self._wh_session is None or self._wh_session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._wh_session = aiohttp.ClientSession(timeout=timeout)
        return self._wh_session

    async def _send_webhook_notification(self, task: AutoPostTask):
        try:
            account = task.account or {}
            channel = task.channel or {}
            target_webhook_url, db_user = await self._resolve_webhook_target(task.user_id, account)
            
            if not target_webhook_url:
                logger.info(
                    "AutoPostManager webhook notification skipped (no webhook) (%s)",
                    _describe_task(task),
                )
                return
            
            logger.info(
                "AutoPostManager sending webhook notification target=%s (%s)",
                _mask_webhook(target_webhook_url),
                _describe_task(task),
            )
            
            sent_count = 0
            if db_user:
                try:
                    acc = next(
                        (
                            acc for acc in db_user.get('accounts', [])
                            if str(acc.get('user_id')) == str(account.get('user_id'))
                        ),
                        None
                    )
                    if acc:
                        ch = next(
                            (
                                ch for ch in acc.get('channels', [])
                                if str(ch.get('channel_id')) == str(channel.get('channel_id'))
                            ),
                            None
                        )
                        if ch:
                            sent_count = ch.get('sent_count', 0)
                except Exception:
                    pass
            
            try:
                base_delay_sec = parse_duration_to_seconds(channel.get('delay', '2m')) or 120
            except Exception:
                base_delay_sec = 120
            
            allowed = True
            if base_delay_sec < 120:
                allowed = isinstance(getattr(task, 'loop_count', None), int) and task.loop_count == 20
            
            if not allowed:
                return
            
            client = self.get_task(task.key)
            if client:
                uptime_seconds = int(time.monotonic() - client.start_time)
            else:
                uptime_seconds = 0
            uptime_str = format_uptime(uptime_seconds)
            timestamp_utc = datetime.now(timezone.utc)
            
            details_lines = [
                f"**<:dot:1426148484146270269> Channel Locate**: <#{channel.get('channel_id', 'N/A')}>",
                f"**<:dot:1426148484146270269> Channel ID**: {channel.get('channel_id', 'N/A')}",
                f"**<:dot:1426148484146270269> Server Name**: {channel.get('server_name', 'N/A')}",
                f"**<:dot:1426148484146270269> Delay**: {channel.get('delay', 'N/A')}"
            ]
            
            rd = (channel.get('delay_random') or '').strip()
            if rd and rd.lower() != 'none':
                details_lines.append(f"**<:dot:1426148484146270269> Random Delay**: {rd}")
            
            embed = {
                "title": "<:clover:1426170416606478437> Message Successfully Sent",
                "description": "Message successfully sent to the destination channel.",
                "color": self.config.get('global_color', 0),
                "fields": [
                    {
                        "name": "Account Usage Details <:user:1426148393092386896>",
                        "value": (
                            f"**<:dot:1426148484146270269> Name**: <@{account.get('user_id', 'N/A')}>\n"
                            f"**<:dot:1426148484146270269> UserID**: {account.get('user_id', 'N/A')}\n"
                            f"**<:dot:1426148484146270269> Nitro**: {'Yes' if account.get('is_nitro') else 'No'}"
                        ),
                        "inline": False
                    },
                    {
                        "name": "Channel Status <:fiworks:1426148388625453091>",
                        "value": (
                            f"**<:dot:1426148484146270269> Channel Uptime**: {uptime_str}\n"
                            f"**<:dot:1426148484146270269> Message Sent**: {sent_count}"
                        ),
                        "inline": False
                    },
                    {
                        "name": "Channel Details <:delete:1426170386734911578>",
                        "value": "\n".join(details_lines),
                        "inline": False
                    }
                ],
                "timestamp": timestamp_utc.isoformat(),
                "footer": {
                    "text": self.config.get('global_footer', ''),
                    "icon_url": self.config.get('global_footer_icon', '')
                }
            }
            
            if channel.get('profile_server'):
                embed['thumbnail'] = {"url": channel.get('profile_server')}
            global_img = self.config.get('global_img_url')
            if global_img:
                embed['image'] = {"url": global_img}
            
            session = self._get_webhook_session()
            async with session.post(target_webhook_url, json={"embeds": [embed]}) as r:
                logger.info(
                    "AutoPostManager webhook notification delivered status=%s (%s)",
                    r.status,
                    _describe_task(task),
                )
                    
        except Exception:
            pass


class AutoPostThread:
    def __init__(self, config, worker_count=2):
        self.config = config
        self.worker_count = worker_count
        self.thread = None
        self.loop = None
        self.manager = None
        self._stop_event = threading.Event()
        self._started = False
        self._ready_event = threading.Event()
        
    def start(self):
        if self._started:
            return
        self._started = True
        self.thread = threading.Thread(target=self._run_thread, daemon=True, name="AutoPostThread")
        self.thread.start()
        self._ready_event.wait(timeout=5.0)
        
    def _run_thread(self):
        # Use a dedicated selector loop here to avoid uvloop/curl_cffi edge cases seen under load.
        self.loop = asyncio.DefaultEventLoopPolicy().new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        try:
            self.loop.run_until_complete(self._async_main())
        except Exception:
            pass
        finally:
            try:
                pending = asyncio.all_tasks(self.loop)
                for task in pending:
                    task.cancel()
                self.loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except:
                pass
            try:
                self.loop.close()
            except:
                pass
    
    async def _async_main(self):
        try:
            self.manager = AutoPostManager(self.config, worker_count=self.worker_count)
            self._ready_event.set()
            
            while not self._stop_event.is_set():
                await asyncio.sleep(1)
                
        except Exception:
            pass
        finally:
            if self.manager:
                try:
                    await self.manager.stop_and_wait()
                except:
                    pass
            self.manager = None
    
    def stop(self):
        if not self._started or self._stop_event.is_set():
            return
        
        self._stop_event.set()
        
        if self.loop and not self.loop.is_closed():
            try:
                if self.manager:
                    future = asyncio.run_coroutine_threadsafe(
                        self.manager.stop_and_wait(),
                        self.loop
                    )
                    try:
                        future.result(timeout=10)
                    except:
                        pass
            except:
                pass
        
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)


_thread_instance: Optional[AutoPostThread] = None


def init_autopost_thread(config, worker_count=2):
    global _thread_instance
    if _thread_instance is None:
        _thread_instance = AutoPostThread(config, worker_count=worker_count)


def get_autopost_thread():
    return _thread_instance


def init_autopost_manager(config, worker_count=1):
    init_autopost_thread(config, worker_count=worker_count)


def get_autopost_manager():
    thread = get_autopost_thread()
    if thread:
        return thread.manager
    return None
