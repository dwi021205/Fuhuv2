import asyncio
import contextlib
import gc
import random
import time
import sys
import threading
from collections import deque
from typing import Optional, Dict, Any, List, Tuple
import ctypes

import selfcord
from selfcord.errors import HTTPException, Forbidden, NotFound, LoginFailure
from selfcord.http import HTTPClient, MultipartParameters

from database import get_db

sys.setrecursionlimit(1000)


async def _run_gc_in_thread(generation: int = 1):
    try:
        await asyncio.to_thread(gc.collect, generation)
    except Exception:
        pass


class AutoReplyClient:
    def __init__(self, owner_id: str, account_cfg: dict, manager):
        self.owner_id = owner_id
        self.account_cfg = account_cfg
        self.manager = manager
        self.token = account_cfg.get("token", "")
        self._tag = f"[{account_cfg.get('username', 'Unknown')}]"
        
        self.client: Optional[selfcord.Client] = None
        self.http: Optional[HTTPClient] = None
        self.self_user_id: Optional[int] = None
        self._running = False
        self._stopped = False
        self._stopping_in_progress = False
        self._stop_reason = "normal"
        
        ar_cfg = account_cfg.get("autoreply", {})
        self.autoreply_message = ar_cfg.get("message", "")
        self._replied_cache = set(ar_cfg.get("replied", []))
        self._error_cache = set(ar_cfg.get("error", []))
        
        presence_str = ar_cfg.get("presence", "online").lower()
        if presence_str == "dnd":
            self.presence_status = selfcord.Status.dnd
        elif presence_str == "idle":
            self.presence_status = selfcord.Status.idle
        elif presence_str == "invisible":
            self.presence_status = selfcord.Status.invisible
        else:
            self.presence_status = selfcord.Status.online
        
        self._message_queue = deque()
        self._process_task: Optional[asyncio.Task] = None
        self._gc_task: Optional[asyncio.Task] = None
        self._runner_task: Optional[asyncio.Task] = None
        self._ready_event: Optional[asyncio.Event] = None
        self._background_tasks_started = False
        self._max_gateway_attempts = 5
        self._gateway_backoff_base = 8
        self._internal_reconnect_enabled = True
        
        self._rate_limit_until = 0
        
        self._manager_notified = False
        self._setup_malloc_trim()
    
    def _setup_malloc_trim(self):
        try:
            self.libc = ctypes.CDLL("libc.so.6")
            self.malloc_trim = self.libc.malloc_trim
            self.malloc_trim.argtypes = [ctypes.c_size_t]
            self.malloc_trim.restype = ctypes.c_int
        except:
            self.libc = None
            self.malloc_trim = None
    
    def _disable_internal_reconnect(self):
        self._internal_reconnect_enabled = False
    
    def _should_use_internal_reconnect(self) -> bool:
        if not self._internal_reconnect_enabled:
            return False
        if self._stop_reason in {"manual", "login_failure", "manager_stopped", "max_retries"}:
            return False
        return True

    def _ensure_background_tasks(self):
        if self._process_task is None or self._process_task.done():
            self._process_task = asyncio.create_task(self._process_message_queue())
        if self._gc_task is None or self._gc_task.done():
            self._gc_task = asyncio.create_task(self._periodic_gc())
        self._background_tasks_started = True
    
    def _create_gateway_client(self) -> selfcord.Client:
        client = selfcord.Client(
            chunk_guilds_at_startup=False,
            guild_subscriptions=False,
            fetch_offline_members=False,
            max_messages=0,
            heartbeat_timeout=60,
            guild_ready_timeout=2
        )
        
        @client.event
        async def on_ready():
            if self._stopped or not self._running:
                return
            
            await self._update_presence()
            if self._ready_event and not self._ready_event.is_set():
                self._ready_event.set()
            self._ensure_background_tasks()
        
        @client.event
        async def on_relationship_add(relationship):
            if self._stopped or not self._running:
                return
            
            if relationship.type.value == 3:
                channel_id = str(relationship.user.dm_channel.id if relationship.user.dm_channel else relationship.user.id)
                if channel_id not in self._replied_cache and channel_id not in self._error_cache:
                    self._message_queue.append(channel_id)
        
        @client.event
        async def on_message(message):
            if self._stopped or not self._running:
                return
            
            if client.user and message.author == client.user:
                return
            
            if not isinstance(message.channel, selfcord.DMChannel):
                return
            
            channel_id = str(message.channel.id)
            if channel_id not in self._replied_cache and channel_id not in self._error_cache:
                self._message_queue.append(channel_id)
        
        return client
    
    async def _wait_for_gateway_ready(self):
        event = self._ready_event
        if not event:
            await asyncio.sleep(0.5)
            return
        try:
            await asyncio.wait_for(event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            pass
    
    async def _run_gateway_with_reconnect(self):
        attempt = 0
        while self._running and not self._stopped:
            self.client = self._create_gateway_client()
            try:
                allow_reconnect = self._should_use_internal_reconnect()
                await self.client.start(self.token, reconnect=allow_reconnect)
                break
            except LoginFailure:
                self._stop_reason = "login_failure"
                self._disable_internal_reconnect()
                raise
            except asyncio.CancelledError:
                raise
            except (selfcord.GatewayNotFound, selfcord.ConnectionClosed, HTTPException, OSError) as exc:
                attempt += 1
                self._stop_reason = "error"
                if attempt >= self._max_gateway_attempts:
                    self._stop_reason = "max_retries"
                    self._disable_internal_reconnect()
                    raise
                backoff = min(self._gateway_backoff_base * attempt, 30)
                await asyncio.sleep(backoff)
            except Exception:
                attempt += 1
                self._stop_reason = "error"
                if attempt >= self._max_gateway_attempts:
                    self._stop_reason = "max_retries"
                    self._disable_internal_reconnect()
                    raise
                backoff = min(self._gateway_backoff_base * attempt, 30)
                await asyncio.sleep(backoff)
            finally:
                if self._ready_event:
                    self._ready_event.clear()
                await self._close_gateway_client()

    def _retry_after_from_exception(self, exc) -> float:
        retry_after = getattr(exc, 'retry_after', None)
        if retry_after:
            try:
                return float(retry_after)
            except (TypeError, ValueError):
                pass
        response = getattr(exc, 'response', None)
        headers = getattr(response, 'headers', {}) if response else {}
        for key in ("Retry-After", "X-RateLimit-Reset-After"):
            value = headers.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return 1.0
    
    async def _optimize_memory(self):
        await _run_gc_in_thread(1)
        
        if self.malloc_trim:
            try:
                await asyncio.to_thread(self.malloc_trim, 0)
            except Exception:
                pass
    
    async def reset_cache(self):
        self._replied_cache.clear()
        self._error_cache.clear()
        await self._sync_cache_from_db()
    
    async def _sync_cache_from_db(self):
        try:
            db = get_db()

            def _sync_read():
                user = db.get_user(self.owner_id)
                if not user:
                    return None
                
                for acc in user.get("accounts", []) or []:
                    if acc.get("token") == self.token:
                        return acc.get("autoreply", {})
                return None

            ar_cfg = await asyncio.to_thread(_sync_read)
            if ar_cfg is None:
                return

            self._replied_cache = set(ar_cfg.get("replied", []))
            self._error_cache = set(ar_cfg.get("error", []))
            self.autoreply_message = ar_cfg.get("message", "")
            
            presence_str = ar_cfg.get("presence", "online").lower()
            if presence_str == "dnd":
                self.presence_status = selfcord.Status.dnd
            elif presence_str == "idle":
                self.presence_status = selfcord.Status.idle
            elif presence_str == "invisible":
                self.presence_status = selfcord.Status.invisible
            else:
                self.presence_status = selfcord.Status.online
        except:
            pass
    
    async def _update_presence(self):
        if not self.client or self._stopped or not self._running:
            return
        
        try:
            await self.client.change_presence(
                status=self.presence_status,
                afk=False
            )
            await asyncio.sleep(0.5)
        except Exception:
            pass
    
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
    
    async def _process_message_queue(self):
        try:
            while self._running and not self._stopped:
                try:
                    if not self._message_queue:
                        await asyncio.sleep(0.5)
                        continue
                    
                    channel_id = self._message_queue.popleft()
                    
                    if channel_id in self._replied_cache or channel_id in self._error_cache:
                        continue
                    
                    current_time = time.time()
                    if current_time < self._rate_limit_until:
                        wait_time = self._rate_limit_until - current_time
                        await asyncio.sleep(wait_time)
                    
                    if not self._running or self._stopped:
                        break
                    
                    if not self.client or not (self._ready_event and self._ready_event.is_set()):
                        self._message_queue.appendleft(channel_id)
                        await self._wait_for_gateway_ready()
                        continue
                    
                    await self._handle_dm_reply(channel_id)
                    
                    delay = random.uniform(3.0, 4.0)
                    await asyncio.sleep(delay)
                    
                except asyncio.CancelledError:
                    break
                except Exception:
                    if not self._running or self._stopped:
                        break
                    await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    async def _send_dm_reply_http(self, channel_id: str) -> Tuple[str, float]:
        if not self.http:
            self._stop_reason = "error"
            self._disable_internal_reconnect()
            asyncio.create_task(self._safe_stop())
            return 'fatal', 0.0
        
        payload = {"content": self.autoreply_message}
        params = MultipartParameters(payload=payload, multipart=None, files=None)
        max_retries = 3
        attempt = 0
        
        while attempt < max_retries:
            try:
                await self.http.send_message(int(channel_id), params=params)
                return 'ok', 0.0
            except HTTPException as e:
                status = getattr(e, 'status', None)
                if status == 429:
                    retry_after = max(1.0, self._retry_after_from_exception(e))
                    self._rate_limit_until = time.time() + retry_after
                    return 'rate_limited', retry_after
                if status == 401:
                    self._stop_reason = "login_failure"
                    self._disable_internal_reconnect()
                    asyncio.create_task(self._safe_stop())
                    return 'fatal', 0.0
                if status == 403:
                    return 'blocked', 0.0
                if status == 400:
                    try:
                        await self.http.accept_message_request(int(channel_id))
                        await self.http.mark_message_request(int(channel_id))
                        await asyncio.sleep(0.25)
                    except Exception:
                        return 'blocked', 0.0
                    attempt += 1
                    continue
                attempt += 1
                await asyncio.sleep(0.5)
                continue
            except Exception:
                attempt += 1
                await asyncio.sleep(0.5)
                continue
        
        self._stop_reason = "error"
        self._disable_internal_reconnect()
        asyncio.create_task(self._safe_stop())
        return 'fatal', 0.0

    async def _handle_dm_reply(self, channel_id: str):
        if self._stopped or not self.client:
            return
        
        try:
            channel = self.client.get_channel(int(channel_id))
            if channel is None:
                channel = await self.client.fetch_channel(int(channel_id))
            
            if not isinstance(channel, selfcord.DMChannel):
                return
            
            try:
                await channel.accept()
            except:
                pass
            
            status, retry_after = await self._send_dm_reply_http(channel_id)
            if status == 'ok':
                self._replied_cache.add(channel_id)
                await self._save_replied_to_db(channel_id)
            elif status == 'rate_limited':
                self._message_queue.appendleft(channel_id)
            elif status in ('blocked', 'error'):
                self._error_cache.add(channel_id)
                await self._save_error_to_db(channel_id)
            else:
                return
            
        except selfcord.RateLimited as e:
            retry_after = getattr(e, 'retry_after', 5.0)
            self._rate_limit_until = time.time() + retry_after
            self._message_queue.appendleft(channel_id)
            
        except (Forbidden, NotFound):
            self._error_cache.add(channel_id)
            await self._save_error_to_db(channel_id)
            
        except HTTPException as e:
            if e.status in [401, 403]:
                self._stop_reason = "login_failure"
                self._disable_internal_reconnect()
                asyncio.create_task(self._safe_stop())
            else:
                self._error_cache.add(channel_id)
                await self._save_error_to_db(channel_id)
                
        except:
            self._error_cache.add(channel_id)
            await self._save_error_to_db(channel_id)
    
    async def _save_replied_to_db(self, channel_id: str):
        if not self.token:
            return
        try:
            db = get_db()
        except Exception:
            return

        def _persist():
            try:
                if hasattr(db, "add_replied_user"):
                    db.add_replied_user(self.owner_id, self.token, channel_id)
                else:
                    user = db.get_user(self.owner_id)
                    if not user:
                        return
                    for acc in user.get("accounts", []) or []:
                        if acc.get("token") == self.token:
                            ar_cfg = acc.setdefault("autoreply", {})
                            replied_list = ar_cfg.setdefault("replied", [])
                            if channel_id not in replied_list:
                                replied_list.append(channel_id)
                            break
                    db.add_user(user)
            except Exception:
                pass

        await asyncio.to_thread(_persist)
    
    async def _save_error_to_db(self, channel_id: str):
        if not self.token:
            return
        try:
            db = get_db()
        except Exception:
            return

        def _persist():
            try:
                if hasattr(db, "add_autoreply_error"):
                    db.add_autoreply_error(self.owner_id, self.token, channel_id)
                else:
                    user = db.get_user(self.owner_id)
                    if not user:
                        return
                    for acc in user.get("accounts", []) or []:
                        if acc.get("token") == self.token:
                            ar_cfg = acc.setdefault("autoreply", {})
                            error_list = ar_cfg.setdefault("error", [])
                            if channel_id not in error_list:
                                error_list.append(channel_id)
                            break
                    db.add_user(user)
            except Exception:
                pass

        await asyncio.to_thread(_persist)
    
    async def start(self):
        if self._running or self._stopped or self._stopping_in_progress:
            return
        
        try:
            self._running = True
            self._stopped = False
            self._stop_reason = "normal"
            self._manager_notified = False
            loop = asyncio.get_running_loop()
            self.http = HTTPClient(loop=loop)
            try:
                me = await self.http.static_login(self.token)
                self.self_user_id = int(me.get("id", 0))
            except LoginFailure:
                self._stop_reason = "login_failure"
                self._disable_internal_reconnect()
                self._running = False
                self._stopped = True
                await self._close_http_client()
                asyncio.create_task(self.manager.on_client_stopped(self, "login_failure"))
                return
            except Exception:
                self._running = False
                self._stopped = True
                await self._close_http_client()
                return
            
            await self._sync_cache_from_db()
            if self._ready_event is None:
                self._ready_event = asyncio.Event()
            
            await self._run_gateway_with_reconnect()
        except selfcord.LoginFailure:
            self._stop_reason = "login_failure"
            await self.stop()
            raise
        except Exception:
            if self._stop_reason == "normal":
                self._stop_reason = "error"
            await self.stop()
            raise
        finally:
            if not self._stopped:
                await self.stop()
    
    async def stop(self):
        if self._stopped or self._stopping_in_progress:
            return
        
        try:
            self._stopping_in_progress = True
            self._running = False
            self._stopped = True
            
            if self._stop_reason in ("manual", "manager_stopped"):
                self._disable_internal_reconnect()
            
            if self._process_task and not self._process_task.done():
                self._process_task.cancel()
                try:
                    await asyncio.wait_for(self._process_task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            
            if self._gc_task and not self._gc_task.done():
                self._gc_task.cancel()
                try:
                    await asyncio.wait_for(self._gc_task, timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            self._background_tasks_started = False
            self._process_task = None
            self._gc_task = None
            
            await self._close_gateway_client()
            
            runner_task = self._runner_task
            if runner_task and runner_task is not asyncio.current_task():
                if not runner_task.done():
                    try:
                        await asyncio.wait_for(runner_task, timeout=5.0)
                    except asyncio.TimeoutError:
                        runner_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await runner_task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
                self._runner_task = None
            
            self._message_queue.clear()
            
            if self._ready_event:
                self._ready_event.clear()
            self._ready_event = None
            
            await self._close_http_client()
            await _run_gc_in_thread()
            
        except Exception:
            pass
        finally:
            self._stopping_in_progress = False
            self._notify_manager_stop()

    def _notify_manager_stop(self):
        if self._manager_notified:
            return
        self._manager_notified = True
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.manager.on_client_stopped(self, self._stop_reason))
        except RuntimeError:
            asyncio.create_task(self.manager.on_client_stopped(self, self._stop_reason))
    
    async def _close_http_client(self):
        http = self.http
        self.http = None
        if not http:
            return
        try:
            await asyncio.wait_for(asyncio.shield(http.close()), timeout=5.0)
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass

    async def _close_gateway_client(self):
        client = self.client
        self.client = None
        if not client:
            return
        try:
            if not client.is_closed():
                await asyncio.wait_for(client.close(), timeout=5.0)
        except asyncio.TimeoutError:
            pass
        except Exception:
            pass
        http = getattr(client, "http", None)
        if http:
            try:
                await asyncio.wait_for(http.close(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass

    async def _safe_stop(self):
        try:
            await asyncio.wait_for(self.stop(), timeout=10.0)
        except asyncio.TimeoutError:
            await self.force_close()
        except Exception:
            await self.force_close()

    async def force_close(self):
        self._stopping_in_progress = True
        self._running = False
        self._stopped = True
        try:
            tasks = [self._process_task, self._gc_task, self._runner_task]
            self._process_task = None
            self._gc_task = None
            self._runner_task = None
            for task in tasks:
                if task and not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            self._message_queue.clear()
            if self._ready_event:
                self._ready_event.clear()
            self._ready_event = None
            try:
                await self._close_gateway_client()
            except Exception:
                pass
            try:
                await self._close_http_client()
            except Exception:
                pass
            await _run_gc_in_thread()
        finally:
            self._stopping_in_progress = False
            self._notify_manager_stop()


class AutoReplyManager:
    def __init__(self, bot, config):
        self.bot = bot
        self.config = config
        self.clients: Dict[str, AutoReplyClient] = {}
        self._lock = asyncio.Lock()
        self._started = False
        self._stopping = False
        self._restart_attempts: Dict[str, int] = {}
        self._restart_delay = 10
        self._max_restart_attempts = 5
    
    async def start(self):
        if self._started:
            return
        
        self._started = True
        self._stopping = False
        
        try:
            db = get_db()
            users = await asyncio.to_thread(db.get_all_users)
        except:
            users = []
        
        for user in users:
            if self._stopping:
                break
            
            owner_id = user.get("user_id")
            for account in user.get("accounts", []) or []:
                if self._stopping:
                    break
                
                ar = account.get("autoreply") or {}
                if ar.get("isactive") and account.get("token"):
                    asyncio.create_task(self.start_task(owner_id, account))
    
    async def stop(self):
        if self._stopping:
            return
        
        self._stopping = True
        
        async with self._lock:
            clients = list(self.clients.values())
            self.clients.clear()
        
        tasks = []
        for client in clients:
            client._stop_reason = "manager_stopped"
            tasks.append(asyncio.create_task(self._safe_stop_client(client)))
        
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10.0)
            except asyncio.TimeoutError:
                pass
            except:
                pass
        
        await _run_gc_in_thread()
    
    async def _safe_stop_client(self, client: AutoReplyClient):
        try:
            await asyncio.wait_for(client.stop(), timeout=8.0)
        except asyncio.TimeoutError:
            await client.force_close()
        except Exception:
            await client.force_close()
    
    async def start_task(self, owner_id: str, account: dict):
        if self._stopping:
            return False
        
        token = account.get("token")
        if not token:
            return False
        
        try:
            async with self._lock:
                if token in self.clients:
                    old_client = self.clients[token]
                    old_client._stop_reason = "restarting"
                    await self._safe_stop_client(old_client)
                
                client = AutoReplyClient(owner_id, account, self)
                self.clients[token] = client
                self._restart_attempts[token] = 0
                
                runner = asyncio.create_task(client.start())
                client._runner_task = runner
                
                def _on_client_done(task: asyncio.Task, *, _client=client, _token=token):
                    try:
                        task.exception()
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
                    finally:
                        if _client._runner_task is task:
                            _client._runner_task = None
                
                runner.add_done_callback(_on_client_done)
            return True
        except Exception:
            async with self._lock:
                self.clients.pop(token, None)
                self._restart_attempts.pop(token, None)
            return False
    
    async def stop_task(self, owner_id: str, token: str):
        async with self._lock:
            client = self.clients.pop(token, None)
        
        if client:
            client._stop_reason = "manual"
            await self._safe_stop_client(client)
        
        self._restart_attempts.pop(token, None)
        await _run_gc_in_thread()
        return client is not None
    
    async def reset_all_caches(self):
        async with self._lock:
            clients = list(self.clients.values())
        
        for client in clients:
            try:
                await client.reset_cache()
            except:
                pass
    
    async def on_client_stopped(self, client: AutoReplyClient, reason: str):
        if self._stopping:
            return
        
        token = client.token
        
        async with self._lock:
            current = self.clients.get(token)
            if current is not client:
                if reason in ("login_failure", "error", "manual", "manager_stopped", "max_retries"):
                    self._restart_attempts.pop(token, None)
                return
            
            if reason in ("manual", "login_failure", "manager_stopped", "max_retries"):
                self.clients.pop(token, None)
        
        
        if reason in ("manual", "manager_stopped"):
            self._restart_attempts.pop(token, None)
            return
        
        if reason == "login_failure":
            await self._deactivate_autoreply(client.owner_id, token)
            self._restart_attempts.pop(token, None)
            return
        
        if not self._started or self._stopping:
            return
        
        db = get_db()
        user = await asyncio.to_thread(db.get_user, client.owner_id)
        if not user:
            self._restart_attempts.pop(token, None)
            return
        
        account_cfg = None
        for acc in user.get("accounts", []) or []:
            if acc.get("token") == token:
                account_cfg = acc
                break
        
        if not account_cfg:
            self._restart_attempts.pop(token, None)
            return
        
        if not (account_cfg.get("autoreply") or {}).get("isactive"):
            self._restart_attempts.pop(token, None)
            return
        
        attempts = self._restart_attempts.get(token, 0) + 1
        self._restart_attempts[token] = attempts
        
        if attempts > self._max_restart_attempts:
            await self._deactivate_autoreply(client.owner_id, token)
            return
        
        asyncio.create_task(self._restart_with_delay(client.owner_id, account_cfg, token))
    
    async def _restart_with_delay(self, owner_id: str, account_cfg: dict, token: str):
        try:
            await asyncio.sleep(self._restart_delay)
            
            if not self._started or self._stopping:
                return
            
            await self.start_task(owner_id, account_cfg)
        except:
            pass
    
    async def _deactivate_autoreply(self, owner_id: str, token: str):
        db = get_db()
        
        def _sync_deactivate():
            user = db.get_user(owner_id)
            if not user:
                return
            
            changed = False
            for acc in user.get("accounts", []) or []:
                if acc.get("token") == token:
                    ar = acc.setdefault("autoreply", {})
                    if ar.get("isactive"):
                        ar["isactive"] = False
                        changed = True
                    break
            
            if changed:
                db.add_user(user)

        await asyncio.to_thread(_sync_deactivate)
        self._restart_attempts.pop(token, None)
    
    async def refresh_all(self):
        if self._stopping:
            return
        
        try:
            db = get_db()
            users = await asyncio.to_thread(db.get_all_users)
        except:
            users = []
        
        async with self._lock:
            existing_clients = list(self.clients.values())
            self.clients.clear()
            self._restart_attempts.clear()
        
        for client in existing_clients:
            client._stop_reason = "refreshing"
            await self._safe_stop_client(client)
        
        if not self._started or self._stopping:
            return
        
        for user in users:
            if self._stopping:
                break
            
            owner_id = user.get("user_id")
            for account in user.get("accounts", []) or []:
                if self._stopping:
                    break
                
                ar = account.get("autoreply") or {}
                if ar.get("isactive") and account.get("token"):
                    asyncio.create_task(self.start_task(owner_id, account))


class AutoReplyThread:
    def __init__(self, bot, config):
        self.bot = bot
        self.config = config
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
        self.thread = threading.Thread(target=self._run_thread, daemon=True, name="AutoReplyThread")
        self.thread.start()
        self._ready_event.wait(timeout=5.0)
        
    def _run_thread(self):
        # Force selector loop to avoid uvloop + curl_cffi websocket FD errors under heavy autoreply load.
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
            self.manager = AutoReplyManager(self.bot, self.config)
            self._ready_event.set()
            
            while not self._stop_event.is_set():
                await asyncio.sleep(1)
                
        except Exception:
            pass
        finally:
            if self.manager:
                try:
                    await self.manager.stop()
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
                        self.manager.stop(),
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


_thread_instance: Optional[AutoReplyThread] = None


def init_autoreply_manager(bot, config):
    global _thread_instance
    if _thread_instance is None:
        _thread_instance = AutoReplyThread(bot, config)


def get_autoreply_manager():
    if _thread_instance:
        return _thread_instance.manager
    return None


def get_autoreply_thread():
    return _thread_instance
