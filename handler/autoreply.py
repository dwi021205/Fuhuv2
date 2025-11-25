# handler/autoreply.py
import asyncio
import logging
from typing import Optional
from discord.ext import commands
from discord import Message, DMChannel

from database import get_db

logger = logging.getLogger(__name__)

# ----- Cog yang menangani DM autoreply -----
class AutoReplyCog(commands.Cog):
    def __init__(self, bot: commands.Bot, default_message: str):
        self.bot = bot
        self.default_message = default_message

    @commands.Cog.listener()
    async def on_message(self, message: Message):
        # Jangan balas bot (termasuk diri sendiri)
        if message.author.bot:
            return

        # Hanya tangani DM
        if not isinstance(message.channel, DMChannel):
            return

        # Optional: kamu bisa tambahkan pengecualian lain di sini
        try:
            # Cek apakah user punya pengaturan autoreply khusus di DB (opsional)
            try:
                db = get_db()
                # asumsikan db.get_autoreply_for_bot() atau struktur sederhana:
                # We'll attempt to find per-bot config in DB; fallback ke global config
                per_message = None
                # contoh: jika kamu menyimpan config global per bot owner, bisa adaptasi
                # kita coba baca semua users and find bot config (best-effort)
                # but to keep it simple, use default_message
            except Exception:
                per_message = None

            reply_text = per_message or self.default_message or "Halo, terima kasih sudah menghubungi. Kami akan merespon segera."
            # Kirim balasan (ignore errors)
            await message.channel.send(reply_text)
        except Exception:
            logger.exception("Failed to send autoreply DM")

# ----- Manager & Thread-like wrapper (agar kompatibel dengan main.py) -----
class AutoReplyManager:
    def __init__(self, bot: commands.Bot, config: dict):
        self.bot = bot
        self.config = config or {}
        self.cog: Optional[AutoReplyCog] = None
        self._started = False
        self._stopping = False

    async def start(self):
        if self._started:
            return
        self._started = True
        # Ambil pesan autoreply dari config.json (key: autoreply_message)
        message = self.config.get("autoreply_message") or self.config.get("default_autoreply") or "Halo! Terima kasih sudah menghubungi."
        # Pasang cog ke bot (jika belum)
        try:
            self.cog = AutoReplyCog(self.bot, message)
            # Jika sudah ada cog serupa sebelumnya, remove dulu (safety)
            for existing in list(self.bot.cogs.keys()):
                if existing == "AutoReplyCog":
                    try:
                        self.bot.remove_cog(existing)
                    except Exception:
                        pass
            # Add cog
            self.bot.add_cog(self.cog)
            logger.info("AutoReplyCog added to bot")
        except Exception:
            logger.exception("Failed to add AutoReplyCog")

    async def stop(self):
        if not self._started or self._stopping:
            return
        self._stopping = True
        try:
            if self.cog:
                # remove cog by class name if present
                try:
                    self.bot.remove_cog(self.cog.__class__.__name__)
                except Exception:
                    # fallback: try remove by name "AutoReplyCog"
                    try:
                        self.bot.remove_cog("AutoReplyCog")
                    except:
                        pass
                self.cog = None
            self._started = False
        except Exception:
            logger.exception("Error while stopping AutoReplyManager")
        finally:
            self._stopping = False

    # Compatibility API (stubs) used by existing code paths
    async def start_task(self, owner_id: str, account: dict):
        # In bot-based autoreply there is no per-token client; return False to indicate no per-account client created
        return False

    async def stop_task(self, owner_id: str, token: str):
        return False

    async def reset_all_caches(self):
        # nothing to cache for this simple implementation
        return True

    async def refresh_all(self):
        # reload config message (in case config changed on disk/db)
        try:
            if self.cog:
                new_msg = self.config.get("autoreply_message") or self.config.get("default_autoreply")
                if new_msg:
                    self.cog.default_message = new_msg
        except Exception:
            pass

# Thread-like wrapper to match original API
_thread_instance: Optional["AutoReplyThread"] = None

class AutoReplyThread:
    def __init__(self, bot, config):
        self.bot = bot
        self.config = config or {}
        self.manager: Optional[AutoReplyManager] = None
        self._started = False
        self._stop_event = asyncio.Event()

    def start(self):
        """
        Called synchronously from main.setup_hook. Since setup_hook runs in the bot loop,
        we can schedule the async initializer directly.
        """
        if self._started:
            return
        loop = asyncio.get_running_loop()
        # schedule the async main function on the same loop
        loop.create_task(self._async_main())
        self._started = True

    async def _async_main(self):
        try:
            self.manager = AutoReplyManager(self.bot, self.config)
            # start manager (adds cog)
            await self.manager.start()
            # Wait until stop event is set
            await self._stop_event.wait()
        except Exception:
            logger.exception("AutoReplyThread encountered an error")
        finally:
            if self.manager:
                try:
                    await self.manager.stop()
                except:
                    pass
            self.manager = None

    def stop(self):
        """
        Trigger stop: set event so _async_main will finish.
        """
        try:
            # If called from non-loop thread, use threadsafe call
            try:
                loop = asyncio.get_running_loop()
                # if we are in same loop, set directly
                if not loop.is_closed():
                    self._stop_event.set()
                    return
            except RuntimeError:
                # no running loop in this thread; use bot loop
                pass

            if self.bot and hasattr(self.bot, "loop") and self.bot.loop and not self.bot.loop.is_closed():
                try:
                    asyncio.run_coroutine_threadsafe(self._set_stop_event(), self.bot.loop)
                except Exception:
                    pass
        except Exception:
            pass

    async def _set_stop_event(self):
        self._stop_event.set()

# API functions expected by main.py
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
