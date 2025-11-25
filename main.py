import platform
import sys
import asyncio
from contextlib import asynccontextmanager
import logging
import threading
import traceback
from urllib.parse import urlparse
import functools

try:
    import resource
except ModuleNotFoundError:
    resource = None

LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s: %(message)s"
LOG_FILE = "error.log"

class UnknownInteractionFilter(logging.Filter):
    UNKNOWN_CODE = 10062

    @staticmethod
    def _contains_unknown_interaction(value: str | None) -> bool:
        return bool(value and "unknown interaction" in value.lower())

    def filter(self, record: logging.LogRecord) -> bool:
        if self._contains_unknown_interaction(record.getMessage()):
            return False

        exc_info = record.exc_info
        if exc_info:
            exc = exc_info[1]
            if exc and getattr(exc, "code", None) == self.UNKNOWN_CODE:
                if self._contains_unknown_interaction(str(exc)):
                    return False

        exc_text = getattr(record, "exc_text", None)
        return not self._contains_unknown_interaction(exc_text)

_UNKNOWN_INTERACTION_FILTER = UnknownInteractionFilter()

def _configure_logging():
    formatter = logging.Formatter(LOG_FORMAT)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.ERROR)
    console_handler.addFilter(_UNKNOWN_INTERACTION_FILTER)

    handlers = [console_handler]
    try:
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.ERROR)
        file_handler.addFilter(_UNKNOWN_INTERACTION_FILTER)
        handlers.append(file_handler)
    except Exception as handler_error:
        print(f"[logging] Failed to attach file handler: {handler_error}", flush=True)

    logging.basicConfig(level=logging.ERROR, handlers=handlers, force=True)
    logging.getLogger().addFilter(_UNKNOWN_INTERACTION_FILTER)

_configure_logging()
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logging.getLogger().setLevel(logging.ERROR)

SILENCED_LOGGERS = ("database", "commands", "handler.autoreply", "handler.autopost")
for _logger_name in SILENCED_LOGGERS:
    _muted_logger = logging.getLogger(_logger_name)
    _muted_logger.handlers.clear()
    _muted_logger.addHandler(logging.NullHandler())
    _muted_logger.propagate = False


def console_info(message: str):
    print(message, flush=True)


def _is_mongo_backup_enabled(config: dict) -> bool:
    flag = str(config.get('mongo_backup_enabled', '')).lower()
    return bool(config.get('mongo_uri')) and flag in ('1', 'true', 'yes', 'on')


def _sanitize_global_image_url(value):
    if not isinstance(value, str):
        return None
    url = value.strip()
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc:
        logger.warning("Ignoring invalid global_img_url in config: %s", url)
        return None
    return url

if not hasattr(asyncio, "timeout"):
    @asynccontextmanager
    async def _compat_timeout(delay: float):
        loop = asyncio.get_running_loop()
        current = asyncio.current_task()
        cancelled_by_timer = False

        def _cancel_current():
            nonlocal cancelled_by_timer
            cancelled_by_timer = True
            if current:
                current.cancel()

        handle = loop.call_later(delay, _cancel_current)
        try:
            yield
        except asyncio.CancelledError as exc:
            if cancelled_by_timer:
                raise asyncio.TimeoutError from exc
            raise
        finally:
            handle.cancel()
    asyncio.timeout = _compat_timeout

if platform.system() == 'Windows':
    if sys.version_info >= (3, 8):
        try:
            logger.info("Windows: Using default ProactorEventLoopPolicy.")
        except AttributeError:
            logger.warning("Warning: Could not set WindowsSelectorEventLoopPolicy")
            
# ==========================
# UVLOOP DISABLED (TERMUX / ANDROID)
# ==========================

if platform.system() == "Windows":
    # Windows tidak memakai uvloop
    logger.info("Windows: uvloop tidak digunakan (ProactorEventLoop sudah default).")
else:
    # Non-Windows & Termux/Android → uvloop dimatikan
    logger.info("uvloop disabled (skipped on this platform).")

import discord
from discord.ext import commands, tasks
from discord import app_commands
import json
import os
import signal
import pytz
import gc
from datetime import datetime, time, timezone
import time as a_time
import aiohttp
import platform as _platform
from commands.utils import close_shared_http_session


_RETRYABLE_INTERACTION_ERRORS = (aiohttp.ClientError, asyncio.TimeoutError)


def _install_interaction_response_retry_patch(max_attempts: int = 3, base_delay: float = 0.35):
    """Add retry logic around critical interaction response methods."""
    try:
        from discord.interactions import InteractionResponse
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.warning("Unable to patch InteractionResponse for retries: %s", exc)
        return

    if getattr(InteractionResponse, "_autoretry_patch_installed", False):
        return

    def _wrap_method(method_name: str):
        original = getattr(InteractionResponse, method_name, None)
        if original is None:
            return

        @functools.wraps(original)
        async def wrapper(self, *args, __orig=original, __method=method_name, **kwargs):
            delay = base_delay
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await __orig(self, *args, **kwargs)
                except _RETRYABLE_INTERACTION_ERRORS as exc:
                    last_exc = exc
                    if attempt >= max_attempts:
                        logger.error(
                            "InteractionResponse.%s failed after %s attempts", __method, attempt, exc_info=exc
                        )
                        raise
                    try:
                        await asyncio.sleep(delay)
                    except Exception:
                        pass
                    delay = min(delay * 2, 2.5)
            if last_exc:
                raise last_exc

        setattr(InteractionResponse, method_name, wrapper)

    for method_name in ("defer",):
        _wrap_method(method_name)

    InteractionResponse._autoretry_patch_installed = True


_install_interaction_response_retry_patch()


def _sync_stop_all_db_tasks():
    db = get_db()
    db.stop_all_active_channels()
    db.stop_all_active_autoreplies()

def _bump_nofile_limit(target: int = 262144):
    if resource is None:
        if platform.system() == 'Windows':
            logger.info("Windows platform without 'resource' module; skipping NOFILE adjustment")
        else:
            logger.warning("NOFILE limit unchanged: 'resource' module is unavailable.")
        return
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        new_soft = min(max(soft, int(target)), hard if hard != resource.RLIM_INFINITY else int(target))
        if new_soft != soft:
            resource.setrlimit(resource.RLIMIT_NOFILE, (new_soft, hard))
            logger.info("Raised NOFILE soft limit: %s -> %s (hard=%s)", soft, new_soft, hard)
        else:
            logger.info("NOFILE soft limit already sufficient (%s, hard=%s)", soft, hard)
    except Exception as e:
        logger.exception("NOFILE limit unchanged: %s", e)

def _log_bot_failure(stage: str, detail: str, exc: Exception | None = None, retry_delay: int | None = None):
    timestamp = datetime.now(timezone.utc)
    try:
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        localized_ts = timestamp.astimezone(jakarta_tz)
    except Exception:
        localized_ts = timestamp

    logger.error(
        "BOT FAILURE stage=%s detail=%s retry_in=%s exc=%s",
        stage,
        detail,
        retry_delay,
        type(exc).__name__ if exc else "none",
        exc_info=exc,
    )

    if exc:
        logger.error("BOT FAILURE trace", exc_info=exc)

    console_msg = [
        "[Bot Watchdog]",
        f"Waktu : {localized_ts.isoformat()}",
        f"Skenario : {stage}",
        f"Detail : {detail}",
    ]
    if retry_delay:
        console_msg.append(f"Tindakan : mencoba ulang dalam {retry_delay}s")
    else:
        console_msg.append("Tindakan : menghentikan bot (tidak ada retry)")
    console_msg.append(f"Exception : {type(exc).__name__ if exc else 'none'}")
    console_info(" | ".join(console_msg))

    if exc:
        exc_only = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        tb_summary = None
        if exc.__traceback__:
            frames = traceback.extract_tb(exc.__traceback__)
            if frames:
                last = frames[-1]
                tb_summary = f"{os.path.basename(last.filename)}:{last.lineno} in {last.name}"
        if tb_summary:
            console_info(f"[Bot Watchdog] Lokasi error: {tb_summary}")
        if exc_only:
            console_info(f"[Bot Watchdog] Rincian error: {exc_only}")

from database import init_database, get_db, backup_to_mongo, DB_SQLITE_FILENAME
from handler.autopost import init_autopost_manager, get_autopost_manager, get_autopost_thread
from handler.autoreply import init_autoreply_manager, get_autoreply_manager, get_autoreply_thread
from handler.status import init_status_manager, get_status_manager
from handler.notification import init_notification_dispatcher, get_notification_dispatcher


class MyBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = self.load_config()
        self.start_time = a_time.time()
        self.invalidated_tokens = set()
        self.jakarta_tz = pytz.timezone('Asia/Jakarta')
        self.reset_time = time(hour=13, minute=0, tzinfo=self.jakarta_tz)
        self._shutdown_in_progress = False
        self.autopost_thread = None
        self.autoreply_thread = None
        self.autopost_manager = None
        self.autoreply_manager = None
        self.status_manager = None
        self._login_banner_printed = False

    def load_config(self):
        config_path = "config.json"
        logger.info("Loading bot configuration from %s", config_path)
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                color = config_data.get('global_color')
                if isinstance(color, str):
                    try:
                        config_data['global_color'] = int(color.lstrip('#'), 16)
                    except (ValueError, TypeError):
                        logger.error("Invalid 'global_color' in config.json. Using default color 0.")
                        config_data['global_color'] = 0
                config_data['global_img_url'] = _sanitize_global_image_url(config_data.get('global_img_url'))
                logger.info(
                    "Configuration loaded: version=%s, mongo_enabled=%s, autopost=%s",
                    config_data.get("bot_version", "unknown"),
                    bool(config_data.get("mongo_uri")),
                    bool(config_data.get("autopost_enabled", True)),
                )
                return config_data
        except FileNotFoundError:
            logger.error("config.json not found. Please create it.")
            exit()
        except json.JSONDecodeError:
            logger.error("config.json is not valid JSON.")
            exit()

    async def setup_hook(self):
        gc.enable()
        gc.set_threshold(200, 3, 3)
        logger.info("setup_hook: GC enabled with thresholds %s", gc.get_threshold())
        
        self.db = get_db()
        await asyncio.to_thread(_sync_stop_all_db_tasks)
        console_info("Stopping All Autoreply & Autopost")
        
        init_status_manager(self, self.config)
        self.status_manager = get_status_manager()
        logger.info("setup_hook: Status manager initialized=%s", bool(self.status_manager))
        
        cogs_path = "commands"
        logger.info("setup_hook: Loading cogs from %s", cogs_path)
        for filename in os.listdir(cogs_path):
            if filename.endswith(".py") and not filename.startswith("__"):
                cog_name = f"commands.{filename[:-3]}"
                try:
                    await self.load_extension(cog_name)
                    logger.info("setup_hook: Loaded cog %s", cog_name)
                except Exception as e:
                    logger.exception("Failed to load cog %s: %s", cog_name, e)
        try:
            synced = await self.tree.sync()
            console_info(f"Sync {len(synced)} Commands")
        except Exception as e:
            console_info("Sync Commands : ERROR (lihat log)")
            logger.exception("Failed to sync commands: %s", e)
            
        self.reset_replied_lists.start()
        self.check_discounts.start()
        self.enforce_trial_expirations.start()
        mongo_enabled = _is_mongo_backup_enabled(self.config)
        if mongo_enabled:
            self.backup_to_mongodb.start()
            logger.info("setup_hook: MongoDB backup task enabled")
        logger.info("setup_hook: Scheduled loops started (reset_replied_lists, check_discounts, enforce_trial_expirations)")
        
        self.status_manager.status_updater.start()
        logger.info("setup_hook: Status updater loop started")
        try:
            if hasattr(self.status_manager, 'speedtest_refresher'):
                self.status_manager.speedtest_refresher.start()
                logger.info("setup_hook: Speedtest refresher started")
        except RuntimeError:
            pass

        loop = asyncio.get_running_loop()
        loop.set_exception_handler(self._handle_exception)
        logger.info("setup_hook: Installed custom loop exception handler")
        
        console_info("Starting Autopost Thread...")
        init_autopost_manager(self.config, worker_count=2)
        self.autopost_thread = get_autopost_thread()
        if self.autopost_thread:
            self.autopost_thread.start()
            logger.info("setup_hook: Autopost thread started")
        
        console_info("Starting Autoreply Thread...")
        init_autoreply_manager(self, self.config)
        self.autoreply_thread = get_autoreply_thread()
        if self.autoreply_thread:
            self.autoreply_thread.start()
            logger.info("setup_hook: Autoreply thread started")
        
        await asyncio.sleep(2)
        
        self.autopost_manager = get_autopost_manager()
        self.autoreply_manager = get_autoreply_manager()
        logger.info(
            "setup_hook: Manager references captured (autopost=%s autoreply=%s)",
            bool(self.autopost_manager),
            bool(self.autoreply_manager),
        )

    def _handle_exception(self, loop, context):
        exception = context.get('exception')
        task = context.get('task') or context.get('future')
        if exception:
            if not self._shutdown_in_progress:
                logger.exception(
                    "Uncaught exception in asyncio task %s (loop=%s): %s",
                    getattr(task, "get_name", lambda: "unknown")(),
                    id(loop),
                    exception,
                )
        else:
            if not self._shutdown_in_progress:
                logger.error("Uncaught asyncio error (loop=%s): %s", id(loop), context.get('message', 'Unknown error'))
        
        if 'future' in context and not self._shutdown_in_progress:
            try:
                context['future'].cancel()
            except Exception:
                pass
        if not self._shutdown_in_progress:
            logger.debug("Loop context payload: %s", context)

    async def on_error(self, event_method, *args, **kwargs):
        if not self._shutdown_in_progress:
            logger.exception("An error occurred in %s: args=%s kwargs=%s", event_method, args, kwargs)

    async def on_ready(self):
        logger.info("Bot logged in as %s (%s)", self.user, self.user.id)
        if not self._login_banner_printed:
            console_info(f"Logged As {self.user}")
            console_info("----------------------------")
            self._login_banner_printed = True
        
        await asyncio.sleep(1)
        
        if self.autopost_thread and self.autopost_thread.loop:
            try:
                if self.autopost_manager:
                    logger.info("Scheduling autopost manager start")
                    asyncio.run_coroutine_threadsafe(
                        self._start_autopost(),
                        self.autopost_thread.loop
                    )
            except Exception as e:
                logger.exception("Error starting autopost: %s", e)
        
        if self.autoreply_thread and self.autoreply_thread.loop:
            try:
                if self.autoreply_manager:
                    logger.info("Scheduling autoreply manager start")
                    asyncio.run_coroutine_threadsafe(
                        self.autoreply_manager.start(),
                        self.autoreply_thread.loop
                    )
            except Exception as e:
                logger.exception("Error starting autoreply: %s", e)
        
        await self.change_presence(activity=discord.Game(name="Fuhuu AutoPost V2"))
        logger.info("Presence updated")
    
    async def _start_autopost(self):
        if self.autopost_manager:
            logger.info("Autopost manager start invoked")
            self.autopost_manager.start()

    async def close(self):
        if self._shutdown_in_progress:
            return
        
        self._shutdown_in_progress = True
        logger.info("Shutting down bot...")

        try:
            if hasattr(self, 'reset_replied_lists') and self.reset_replied_lists.is_running():
                self.reset_replied_lists.cancel()
            if hasattr(self, 'check_discounts') and self.check_discounts.is_running():
                self.check_discounts.cancel()
            if hasattr(self, 'backup_to_mongodb') and self.backup_to_mongodb.is_running():
                self.backup_to_mongodb.cancel()
            logger.info("Cancelled scheduled background tasks")
        except Exception:
            pass

        try:
            await asyncio.to_thread(_sync_stop_all_db_tasks)
            logger.info("Marked DB tasks inactive during shutdown")
        except Exception:
            pass

        try:
            if self.autopost_thread:
                self.autopost_thread.stop()
                logger.info("Autopost thread stopped")
        except Exception:
            pass

        try:
            if self.autoreply_thread:
                self.autoreply_thread.stop()
                logger.info("Autoreply thread stopped")
        except Exception:
            pass

        try:
            if self.status_manager and hasattr(self.status_manager, 'status_updater') and self.status_manager.status_updater.is_running():
                self.status_manager.status_updater.cancel()
                logger.info("Status updater cancelled")
        except Exception:
            pass

        try:
            if self.status_manager and hasattr(self.status_manager, 'speedtest_refresher') and self.status_manager.speedtest_refresher.is_running():
                self.status_manager.speedtest_refresher.cancel()
                logger.info("Speedtest refresher cancelled")
        except Exception:
            pass

        try:
            db_instance = get_db()
            close_fn = getattr(db_instance, 'close_all_connections', None)
            if callable(close_fn):
                close_fn()
                logger.info("Database connections closed")
        except Exception:
            pass

        try:
            await close_shared_http_session()
            logger.info("Shared HTTP session closed")
        except Exception:
            pass

        await asyncio.sleep(1)

        try:
            await super().close()
            logger.info("discord.py client closed cleanly")
        except Exception:
            pass

    async def handle_invalid_token(self, user_id, token, account):
        if token in self.invalidated_tokens:
            return
        self.invalidated_tokens.add(token)
        logger.info(
            "handle_invalid_token: user=%s account=%s token=%.8s",
            user_id,
            account.get('username'),
            (token or "")[:8],
        )
        
        try:
            manager = self.autopost_manager
            if manager and self.autopost_thread and self.autopost_thread.loop:
                for acc in get_db().get_user(user_id).get('accounts', []):
                    if acc['token'] == token:
                        for channel in acc.get('channels', []):
                            task_key = f"{token}_{channel['channel_id']}"
                            logger.info("Removing autopost task %s due to invalid token", task_key)
                            asyncio.run_coroutine_threadsafe(
                                manager.remove_task(task_key),
                                self.autopost_thread.loop
                            )
            
            autoreply_manager = self.autoreply_manager
            if autoreply_manager and self.autoreply_thread and self.autoreply_thread.loop:
                logger.info("Stopping autoreply task for user=%s token=%.8s", user_id, (token or "")[:8])
                asyncio.run_coroutine_threadsafe(
                    autoreply_manager.stop_task(user_id, token),
                    self.autoreply_thread.loop
                )
        except:
            pass
        
        try:
            embed = discord.Embed(
                title="Account Token Invalid",
                description=f"The token for your account **{account['username']}** has expired or is invalid. All automated tasks for this account have been stopped. Please update the token using the `/account` command.",
                color=0xff0000,
                timestamp=datetime.now(pytz.utc)
            )
            embed.set_footer(text=self.config.get('global_footer', ''), icon_url=self.config.get('global_footer_icon', ''))
            img_url = self.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)

            dispatcher = get_notification_dispatcher()
            if dispatcher:
                await dispatcher.enqueue_dm({"user_id": user_id, "embed": embed})
                logger.info("Queued invalid token DM for user %s", user_id)
            else:
                owner = await self.fetch_user(user_id)
                if owner:
                    await owner.send(embed=embed)
                    logger.info("Notified user %s about invalid token", user_id)
        except (discord.NotFound, discord.Forbidden):
            logger.warning("Could not notify user %s about invalid token (DM blocked)", user_id)
        finally:
            self.invalidated_tokens.discard(token)

    def check_user_expiration(self, user_id, user_config=None):
        db = get_db()
        user_config = user_config or db.get_user(str(user_id))
        if not user_config:
            return False

        package_type = (user_config.get('package_type') or '').lower()
        expiry_date = user_config.get('expiry_date')

        if not user_config.get('is_active', True):
            return False

        if 'trial' not in package_type or not expiry_date:
            return True

        if expiry_date.tzinfo is None:
            expiry_date = expiry_date.replace(tzinfo=timezone.utc)

        if datetime.now(timezone.utc) <= expiry_date:
            return True

        for account in user_config.get('accounts', []) or []:
            account['is_active'] = False
            for channel in account.get('channels', []) or []:
                channel['is_active'] = False
        user_config['is_active'] = False
        db.add_user(user_config)
        user_key = str(user_config.get('user_id') or user_id)
        logger.info("Marked trial user %s inactive (expired at %s)", user_key, expiry_date.isoformat())

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._handle_trial_expiration(user_config, expiry_date))
        except RuntimeError:
            target_loop = getattr(self, 'loop', None)
            if target_loop and target_loop.is_running():
                asyncio.run_coroutine_threadsafe(self._handle_trial_expiration(user_config, expiry_date), target_loop)
            else:
                asyncio.run(self._handle_trial_expiration(user_config, expiry_date))

        return False

    async def _handle_trial_expiration(self, user_config, expiry_date):
        try:
            await self._stop_user_automations(user_config)
        except Exception:
            logger.exception("Failed to stop automations for expired user %s", user_config.get('user_id'))
        try:
            await self._notify_trial_expired(user_config, expiry_date)
        except Exception:
            logger.exception("Failed to notify expired user %s", user_config.get('user_id'))

    async def _stop_user_automations(self, user_config):
        user_id = str(user_config.get('user_id') or '')
        if not user_id:
            return
        manager_post = get_autopost_manager()
        manager_reply = get_autoreply_manager()
        for account in user_config.get('accounts', []) or []:
            token = account.get('token')
            if manager_reply and token and self.autoreply_thread and self.autoreply_thread.loop:
                try:
                    future = asyncio.run_coroutine_threadsafe(
                        manager_reply.stop_task(user_id, token),
                        self.autoreply_thread.loop
                    )
                    future.result(timeout=5)
                except Exception:
                    logger.exception("Failed to stop autoreply for %s", user_id)
            if not manager_post or not token:
                continue
            for channel in account.get('channels', []) or []:
                channel_id = channel.get('channel_id')
                if not channel_id:
                    continue
                task_key = f"{token}_{channel_id}"
                try:
                    if self.autopost_thread and self.autopost_thread.loop:
                        future = asyncio.run_coroutine_threadsafe(
                            manager_post.remove_task(task_key),
                            self.autopost_thread.loop
                        )
                        future.result(timeout=5)
                except Exception:
                    logger.exception("Failed to stop autopost %s for %s", task_key, user_id)

    async def _notify_trial_expired(self, user_config, expiry_date):
        user_id = str(user_config.get('user_id') or '')
        if not user_id:
            return
        try:
            owner = await self.fetch_user(int(user_id))
        except (discord.NotFound, discord.HTTPException, ValueError):
            return
        package_name = user_config.get('package_type') or 'Unknown Package'
        expiry_value = ''
        if isinstance(expiry_date, datetime):
            if expiry_date.tzinfo is None:
                expiry_date = expiry_date.replace(tzinfo=timezone.utc)
            expiry_value = f"<t:{int(expiry_date.timestamp())}:F>"
        embed = discord.Embed(
            title="Your Subscription Has Expired",
            description=(
                "All of your AutoPost and AutoReply tasks were stopped because the trial package is no longer active. "
                "Please renew to resume automation."
            ),
            color=self.config.get('global_color', 0xff0000),
            timestamp=datetime.now(timezone.utc)
        )
        embed.add_field(name="Package", value=package_name, inline=False)
        if expiry_value:
            embed.add_field(name="Expired On", value=expiry_value, inline=False)
        embed.add_field(
            name="Next Steps",
            value="Use `/shop buy` or contact the administrator to activate a new package.",
            inline=False
        )
        embed.set_footer(text=self.config.get('global_footer', ''), icon_url=self.config.get('global_footer_icon', ''))
        img_url = self.config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)
        try:
            await owner.send(f"{owner.mention} Your automation package expired, please review the details below.")
            await owner.send(embed=embed)
        except (discord.Forbidden, discord.HTTPException):
            logger.warning("Could not send expiration DM to user %s", user_id)

    @tasks.loop(time=time(hour=2, minute=0, tzinfo=pytz.timezone('Asia/Jakarta')))
    async def enforce_trial_expirations(self):
        try:
            db = get_db()
            users = await asyncio.to_thread(db.get_all_users)
            for user in users or []:
                try:
                    self.check_user_expiration(user.get('user_id'), user_config=user)
                except Exception:
                    logger.exception("Failed trial validation sweep for user %s", user.get('user_id'))
        except Exception as e:
            logger.exception("Global trial expiration sweep failed: %s", e)

    @enforce_trial_expirations.before_loop
    async def before_enforce_trial_expirations(self):
        await self.wait_until_ready()

    @tasks.loop(time=time(hour=1, minute=0, tzinfo=pytz.timezone('Asia/Jakarta')))
    async def reset_replied_lists(self):
        try:
            await asyncio.to_thread(get_db().reset_all_replied_lists)
            logger.info("Scheduled reset_replied_lists executed successfully")
            
            if self.autoreply_thread and self.autoreply_thread.loop and self.autoreply_manager:
                asyncio.run_coroutine_threadsafe(
                    self.autoreply_manager.reset_all_caches(),
                    self.autoreply_thread.loop
                )
                logger.info("Requested autoreply cache reset after DB cleanup")
        except Exception as e:
            logger.exception("Error during scheduled replied list reset: %s", e)

    @reset_replied_lists.before_loop
    async def before_reset_replied_lists(self):
        await self.wait_until_ready()

    @tasks.loop(minutes=30)
    async def check_discounts(self):
        try:
            db = get_db()
            
            def _sync_check_discounts():
                discount = db.get_discount()
                if discount and discount.get('expiry'):
                    expiry_dt = discount['expiry']
                    if expiry_dt.tzinfo is None:
                        expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
                    if datetime.now(timezone.utc) > expiry_dt:
                        db.remove_discount()
                        logger.info("Expired discount removed (expired at %s)", expiry_dt.isoformat())
                else:
                    logger.debug("No active discount to validate in this cycle")

            await asyncio.to_thread(_sync_check_discounts)
            
        except Exception as e:
            logger.exception("Error during scheduled discount check: %s", e)

    @check_discounts.before_loop
    async def before_check_discounts(self):
        await self.wait_until_ready()

    @tasks.loop(time=time(hour=2, minute=0, tzinfo=pytz.timezone('Asia/Jakarta')))
    async def backup_to_mongodb(self):
        try:
            logger.info("Daily backup to MongoDB started")
            success = await asyncio.to_thread(backup_to_mongo)
            if success:
                logger.info("Daily backup to MongoDB completed successfully")
            else:
                logger.error("Daily backup failed.")
        except Exception as e:
            logger.exception("Daily backup error: %s", e)

    @backup_to_mongodb.before_loop
    async def before_backup_to_mongodb(self):
        await self.wait_until_ready()

async def main():
    _bump_nofile_limit()
    intents = discord.Intents.default()
    intents.message_content = True
    bot = MyBot(command_prefix="!", intents=intents)
    shutdown_lock = asyncio.Lock()
    shutdown_started = False

    backup_enabled = _is_mongo_backup_enabled(bot.config)
    console_info("Checking Database .....")
    db_exists = os.path.exists(DB_SQLITE_FILENAME)
    try:
        init_database(bot.config)
        db = get_db()
        try:
            schema_version = db.get_schema_version()
        except Exception:
            schema_version = "unknown"
        try:
            user_count = db.count_users_fast()
        except Exception:
            user_count = "unknown"
        origin_note = "existing database detected" if db_exists else "new database initialized"
        console_info(f"Database Status : OK ({origin_note}; schema={schema_version}; users={user_count})")
        _sync_stop_all_db_tasks()
    except Exception as e:
        console_info(f"Database Status : ERROR ({e})")
        console_info("Action : Periksa autopost.db atau konfigurasi MongoDB sebelum menjalankan ulang.")
        logger.exception("Preflight initialization error: %s", e)

    if backup_enabled:
        console_info("Backup Status : Enabled (MongoDB)")
    else:
        console_info("Backup Status : Disabled (mongo_uri atau backup flag tidak aktif)")

    async def shutdown_handler(reason: str = "shutdown"):
        nonlocal shutdown_started
        async with shutdown_lock:
            if shutdown_started:
                return
            shutdown_started = True
        logger.info("Shutdown handler triggered (%s)", reason)
        try:
            await asyncio.wait_for(bot.close(), timeout=20.0)
            logger.info("Bot close sequence finished (%s)", reason)
        except asyncio.TimeoutError:
            logger.error("Timed out while waiting for bot shutdown (%s)", reason)
        except Exception:
            logger.exception("Error while executing shutdown handler (%s)", reason)

    def setup_signal_handlers():
        loop = asyncio.get_running_loop()

        def _schedule(reason: str):
            loop.call_soon_threadsafe(lambda: asyncio.create_task(shutdown_handler(reason)))

        def signal_handler(signum, frame):
            try:
                sig_name = signal.Signals(signum).name
            except Exception:
                sig_name = f"signal-{signum}"
            _schedule(sig_name)

        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)

    setup_signal_handlers()
    logger.info("Signal handlers for SIGINT/SIGTERM registered")

    retry_delay = 5
    max_delay = 60
    while True:
        try:
            logger.info("Starting bot login sequence")
            await bot.start(bot.config['bot_token'])
            if shutdown_started:
                break
            _log_bot_failure(
                "unexpected-stop",
                "Client discord.py berhenti tanpa sinyal shutdown. Kemungkinan crash internal atau restart otomatis.",
                retry_delay=retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(max_delay, retry_delay * 2)
            continue
        except discord.errors.LoginFailure as exc:
            _log_bot_failure(
                "login-failure",
                "Token bot tidak valid pada config.json. Proses dihentikan, perbarui token sebelum menjalankan ulang.",
                exc=exc,
            )
            return
        except KeyboardInterrupt:
            await shutdown_handler("keyboard-interrupt")
            return
        except (aiohttp.ClientConnectorError, aiohttp.ClientOSError, aiohttp.ServerTimeoutError, aiohttp.ServerDisconnectedError, asyncio.TimeoutError) as e:
            _log_bot_failure(
                "network-error",
                f"Kesalahan jaringan saat koneksi ke Discord: {e}",
                exc=e,
                retry_delay=retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(max_delay, retry_delay * 2)
            continue
        except OSError as e:
            if getattr(e, 'errno', None) in (-2, 11001) or 'Name or service not known' in str(e):
                _log_bot_failure(
                    "dns-error",
                    f"DNS error saat mencoba menghubungkan ke Discord: {e}",
                    exc=e,
                    retry_delay=retry_delay,
                )
                await asyncio.sleep(retry_delay)
                retry_delay = min(max_delay, retry_delay * 2)
                continue
            else:
                _log_bot_failure(
                    "os-error",
                    f"Kesalahan OS saat menjalankan bot: {e}",
                    exc=e,
                )
                await shutdown_handler("os-error")
                return
        except Exception as e:
            _log_bot_failure(
                "runtime-exception",
                "Exception tidak tertangani pada loop utama bot.",
                exc=e,
                retry_delay=retry_delay,
            )
            await asyncio.sleep(retry_delay)
            retry_delay = min(max_delay, retry_delay * 2)
            continue

if __name__ == "__main__":
    asyncio.run(main())
