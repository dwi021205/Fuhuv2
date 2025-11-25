import discord
from discord import ui, app_commands
from discord.ext import commands
import aiohttp
import psutil
import platform
import time
from datetime import datetime, timezone, timedelta
import asyncio
import json
import contextlib

from database import get_db, run_db
import logging

logger = logging.getLogger(__name__)
logger.handlers.clear()
logger.addHandler(logging.NullHandler())
logger.propagate = False

# Per-event-loop session and lock registries to avoid cross-loop issues
_SESSIONS: dict[asyncio.AbstractEventLoop, aiohttp.ClientSession] = {}
_SESSION_LOCKS: dict[asyncio.AbstractEventLoop, asyncio.Lock] = {}


def _get_loop_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _SESSION_LOCKS.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _SESSION_LOCKS[loop] = lock
    return lock


async def _get_shared_http_session() -> aiohttp.ClientSession:
    loop = asyncio.get_running_loop()
    lock = _get_loop_lock()
    async with lock:
        session = _SESSIONS.get(loop)
        if session is None or session.closed:
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            connector = aiohttp.TCPConnector(limit=64, limit_per_host=16)
            session = aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False)
            _SESSIONS[loop] = session
        return session


async def close_shared_http_session():
    loop = asyncio.get_running_loop()
    lock = _SESSION_LOCKS.get(loop)
    if lock is None:
        return
    async with lock:
        session = _SESSIONS.pop(loop, None)
        if session and not session.closed:
            await session.close()


async def safe_defer_interaction(interaction: discord.Interaction, ephemeral: bool = True):
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
    except Exception as e:
        logger.exception('Unhandled exception in %s', __name__)


async def safe_send_response(interaction: discord.Interaction, content=None, embed=None, view=None, ephemeral=True):
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(
                content=content, embed=embed, view=view, ephemeral=ephemeral
            )
        else:
            await interaction.followup.send(
                content=content, embed=embed, view=view, ephemeral=ephemeral
            )
    except Exception as e:
        logger.exception('Unhandled exception in %s', __name__)
        raise


async def check_user_exists(user_id):
    try:
        db = get_db()
        user_config = await run_db(db.get_user, user_id)
        return user_config is not None
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)
        return False


async def ensure_active_subscription(
    interaction: discord.Interaction,
    *,
    require_accounts: bool = False,
    missing_account_message: str | None = None
):
    db = get_db()
    user_config = await run_db(db.get_user, interaction.user.id)
    if not user_config:
        await safe_send_response(
            interaction,
            content="<:exclamation:1426164277638594680> You are not registered to use this bot. Please contact the owner.",
            ephemeral=True
        )
        return None
    if not interaction.client.check_user_expiration(interaction.user.id, user_config=user_config):
        await safe_send_response(
            interaction,
            content="<:exclamation:1426164277638594680> Your trial subscription has expired. Please contact the owner or use `/shop buy` to purchase a new subscription.",
            ephemeral=True
        )
        return None
    if require_accounts and not user_config.get('accounts'):
        await safe_send_response(
            interaction,
            content=(
                missing_account_message
                or "<:exclamation:1426164277638594680> No Account Configured Using Command `/account` & Use Button `Add Account` to added the account"
            ),
            ephemeral=True
        )
        return None
    return user_config

async def send_test_to_webhook(url: str, message: str = "Validating Message"):
    try:
        if not url:
            return False
        test_url = url
        if "wait=" not in test_url:
            sep = "&" if "?" in test_url else "?"
            test_url = test_url + f"{sep}wait=true"
        headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}
        session = await _get_shared_http_session()
        async with session.post(test_url, json={"content": message}, headers=headers) as res:
            if res.status in (200, 204):
                if res.status == 200:
                    try:
                        data = await res.json(content_type=None)
                        msg_id = data.get("id")
                        if msg_id:
                            delete_url = f"{url}/messages/{msg_id}"
                            async with session.delete(delete_url) as delete_resp:
                                if delete_resp.content_length is None or delete_resp.content_length > 0:
                                    with contextlib.suppress(Exception):
                                        await delete_resp.read()
                    except Exception:
                        logger.exception('Unhandled exception in %s', __name__)
                return True
            return False
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return False

def build_set_webhook_embed(config: dict) -> discord.Embed:
    embed = discord.Embed(
        title="<:server:1426173792144724048> Set Private Webhook",
        description=(
            "How to get a Discord webhook URL:\n"
            "1. Open channel settings → Integrations → Webhooks.\n"
            "2. Create New Webhook (or choose an existing one).\n"
            "3. Copy the Webhook URL and paste it here.\n"
            "Keep your webhook URL private."
        ),
        color=config.get('global_color', 0),
        timestamp=datetime.now(timezone.utc)
    )
    embed.set_footer(text=config.get('global_footer', ''), icon_url=config.get('global_footer_icon', ''))
    img_url = config.get('global_img_url')
    if img_url:
        embed.set_image(url=img_url)
    return embed

class WebhookSettingsView(ui.View):
    def __init__(self, bot):
        super().__init__(timeout=180)
        self.bot = bot

    @ui.button(label="Setting Webhook", style=discord.ButtonStyle.secondary, emoji="<:edit:1426170391218487349>")
    async def open_modal(self, interaction: discord.Interaction, _: ui.Button):
        await interaction.response.send_modal(WebhookSettingsModal())

    @ui.button(label="Delete", style=discord.ButtonStyle.danger, emoji="<:trash:1426173796796203029>")
    async def delete_webhook(self, interaction: discord.Interaction, _: ui.Button):
        await safe_defer_interaction(interaction)
        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> You are not registered to use this bot. Please contact the owner.", ephemeral=True)
            return
        if 'webhookurl' in user_config:
            user_config.pop('webhookurl', None)
            await run_db(db.add_user, user_config)
            await interaction.followup.send("<:exclamation:1426164277638594680> Your private webhook has been removed. Global webhook will be used again.", ephemeral=True)
        else:
            await interaction.followup.send("<:exclamation:1426164277638594680> No private webhook is set for your account.", ephemeral=True)

class WebhookSettingsModal(ui.Modal, title='Webhook Setting'):
    webhook_url_input = ui.TextInput(
        label='WebhookURL',
        style=discord.TextStyle.short,
        placeholder='Input ur URLs webhook server',
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)

            webhook_url = self.webhook_url_input.value.strip()
            ok = await send_test_to_webhook(webhook_url, "<:clover:1426170416606478437> Validating Message")
            if not ok:
                await interaction.followup.send(
                    content='<:exclamation:1426164277638594680> Sorry, the webhook URL could not be validated. Please provide a valid Discord webhook.',
                    ephemeral=True
                )
                return

            db = get_db()
            user_config = await run_db(db.get_user, interaction.user.id)

            if user_config:
                old_url = user_config.get('webhookurl')
                user_config['webhookurl'] = webhook_url
                await run_db(db.add_user, user_config)
                if old_url and old_url != webhook_url:
                    msg = '<:clover:1426170416606478437> Your private log webhook has been updated and verified.'
                elif old_url == webhook_url:
                    msg = '<:clover:1426170416606478437> Your private log webhook is unchanged and verified.'
                else:
                    msg = '<:clover:1426170416606478437> Your private log webhook has been successfully set and verified.'
                await interaction.followup.send(content=msg, ephemeral=True)
            else:
                await interaction.followup.send(
                    content='<:exclamation:1426164277638594680> An error occurred while fetching your user data. Please try again.',
                    ephemeral=True
                )
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send(
                    content='<:exclamation:1426164277638594680> An error occurred while processing your request. Please try again.',
                    ephemeral=True
                )
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

class UtilsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_db()

    utils_group = app_commands.Group(name="utils", description="General utility commands.")

    def _collect_system_metrics(self):
        os_name = f"{platform.system()} {platform.release()}"
        vm = psutil.virtual_memory()
        ram_total = vm.total / (1024**3)
        ram_used = vm.used / (1024**3)
        ram_available = max(ram_total - ram_used, 0)

        disk = psutil.disk_usage('/')
        disk_total = disk.total / (1024**3)
        disk_used = disk.used / (1024**3)
        disk_available = max(disk_total - disk_used, 0)

        os_uptime_seconds = time.time() - psutil.boot_time()
        bot_start = getattr(self.bot, 'start_time', None)
        bot_uptime_seconds = (time.time() - bot_start) if bot_start else 0

        return {
            'os_name': os_name,
            'ram_available': ram_available,
            'disk_available': disk_available,
            'os_uptime_seconds': os_uptime_seconds,
            'bot_uptime_seconds': bot_uptime_seconds,
        }

    @utils_group.command(name="set_webhook_private", description="Set a private webhook for your autopost logs.")
    async def set_webhook_private(self, interaction: discord.Interaction):
        try:
            if not self.bot.check_user_expiration(interaction.user.id):
                await safe_send_response(
                    interaction,
                    content="<:exclamation:1426164277638594680> Your trial subscription has expired. Please contact the owner or use `/shop buy` to purchase a new subscription.",
                    ephemeral=True
                )
                return

            db = get_db()
            user = await run_db(db.get_user, interaction.user.id)
            if not user:
                await safe_send_response(
                    interaction,
                    content="<:exclamation:1426164277638594680> You are not registered to use this bot. Please contact the owner.",
                    ephemeral=True
                )
                return

            embed = build_set_webhook_embed(self.bot.config)
            view = WebhookSettingsView(self.bot)
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        except Exception as e:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await safe_send_response(
                    interaction,
                    content="<:exclamation:1426164277638594680> An error occurred. Please try again.",
                    ephemeral=True
                )
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

    @utils_group.command(name="info", description="Show bot information and your account details.")
    async def utils_info(self, interaction: discord.Interaction):
        try:
            if not self.bot.check_user_expiration(interaction.user.id):
                await safe_send_response(
                    interaction,
                    content="<:exclamation:1426164277638594680> Your trial subscription has expired. Please contact the owner or use `/shop buy` to purchase a new subscription.",
                    ephemeral=True
                )
                return

            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)

            user = interaction.user

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

            db = get_db()
            user_config = await run_db(db.get_user, str(user.id))
            is_registered = user_config is not None

            def fmt_limit(val):
                try:
                    if val is None:
                        return "N/A"
                    if int(val) == -1:
                        return "Unlimited"
                    return str(val)
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
                    return str(val)

            def fmt_expiry(u):
                package_type = (u.get('package_type') or '').lower()
                expiry_date = u.get('expiry_date')
                if 'perma' in package_type:
                    return 'Never'
                if isinstance(expiry_date, datetime):
                    if expiry_date.tzinfo is None:
                        expiry_date = expiry_date.replace(tzinfo=timezone.utc)
                    return f"<t:{int(expiry_date.timestamp())}:F>"
                return str(expiry_date) if expiry_date else 'N/A'

            stats = await asyncio.to_thread(self._collect_system_metrics)
            os_name = stats['os_name']
            ram_available = stats['ram_available']
            disk_available = stats['disk_available']
            os_uptime = format_uptime(stats['os_uptime_seconds'])
            bot_uptime = format_uptime(stats['bot_uptime_seconds'])

            embed = discord.Embed(
                title="<:user:1426148393092386896> Bot Info / User Info",
                description="Here is the bot information and your information.",
                color=self.bot.config.get('global_color')
            )

            display_name = user.global_name or user.display_name or user.name
            user_value = (
                f"**<:dot:1426148484146270269> Display Name** : {display_name}\n"
                f"**<:dot:1426148484146270269> Username** : {user.name}\n"
                f"**<:dot:1426148484146270269> User ID** : {user.id}\n"
                f"**<:dot:1426148484146270269> Registered?** : {'Yes' if is_registered else 'No'}"
            )
            if is_registered:
                status = "Active" if user_config.get('is_active', True) else "Inactive"
                user_value += (
                    f"\n**<:dot:1426148484146270269> Package Type** : {user_config.get('package_type', 'Unknown')}\n"
                    f"**<:dot:1426148484146270269> Limit** : {fmt_limit(user_config.get('limit'))}\n"
                    f"**<:dot:1426148484146270269> Expiry Date** : {fmt_expiry(user_config)}\n"
                    f"**<:dot:1426148484146270269> Status** : {status}"
                )
            embed.add_field(name="User Info <:user:1426148393092386896>", value=user_value, inline=False)

            os_details_value = (
                f"**<:dot:1426148484146270269> Operating Sistem** : {os_name}\n"
                f"**<:dot:1426148484146270269> RAM Avaliable** : {ram_available:.2f} GB\n"
                f"**<:dot:1426148484146270269> Memory Avaliable** : {disk_available:.2f} GB\n"
                f"**<:dot:1426148484146270269> OS Uptime** : {os_uptime}\n"
                f"**<:dot:1426148484146270269> Bot Uptime** : {bot_uptime}"
            )
            embed.add_field(name="OS Details <:computer:1426164290490073208>", value=os_details_value, inline=False)

            img_url = self.bot.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)

            tz = getattr(self.bot, 'jakarta_tz', timezone.utc)
            now = datetime.now(tz)
            time_only = now.strftime('%I:%M %p').lstrip('0')
            human_time = f"Today {time_only}"
            footer_text = f"{self.bot.config.get('global_footer', '')} • {human_time}".strip()
            embed.set_footer(
                text=footer_text,
                icon_url=self.bot.config.get('global_footer_icon', '')
            )

            try:
                await interaction.edit_original_response(embed=embed)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
                await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await safe_send_response(
                    interaction,
                    content="<:exclamation:1426164277638594680> An error occurred while gathering information. Please try again.",
                    ephemeral=True
                )
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

async def setup(bot: commands.Bot):
    await bot.add_cog(UtilsCog(bot))
