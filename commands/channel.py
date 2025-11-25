import discord
from discord import ui, app_commands
from discord.ext import commands
import aiohttp
from datetime import datetime, timezone
import asyncio
import time
import random
import copy
import logging
from database import get_db, run_db
from commands.utils import ensure_active_subscription
from handler.autopost import (
    get_autopost_manager,
    AutoPostTask,
    parse_duration_to_seconds,
    format_uptime,
    format_seconds_to_duration
)

logger = logging.getLogger(__name__)
logger.handlers.clear()
logger.addHandler(logging.NullHandler())
logger.propagate = False


def _ensure_iso(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _sanitize_channel_entry(channel: dict) -> dict:
    if not isinstance(channel, dict):
        return {}
    cleaned = copy.deepcopy(channel)
    cleaned.pop('_validation_status', None)
    cleaned.pop('attachments', None)
    for key, val in list(cleaned.items()):
        if isinstance(val, datetime):
            cleaned[key] = val.isoformat()
    return cleaned


def _sanitize_account_entry(account: dict) -> dict:
    if not isinstance(account, dict):
        return {}
    cleaned = copy.deepcopy(account)
    cleaned.pop('_validation_status', None)
    cleaned['created_at'] = _ensure_iso(cleaned.get('created_at'))
    cleaned['added_at'] = _ensure_iso(cleaned.get('added_at') or datetime.now(timezone.utc))
    channels = cleaned.get('channels', []) or []
    cleaned['channels'] = [_sanitize_channel_entry(ch) for ch in channels]
    return cleaned


def _prepare_user_config_for_storage(user_config: dict) -> dict:
    if not isinstance(user_config, dict):
        return {}
    sanitized = copy.deepcopy(user_config)
    sanitized['expiry_date'] = _ensure_iso(sanitized.get('expiry_date'))
    sanitized['updated_at'] = _ensure_iso(sanitized.get('updated_at'))
    accounts = sanitized.get('accounts', []) or []
    sanitized['accounts'] = [_sanitize_account_entry(acc) for acc in accounts]
    return sanitized
async def validate_token_for_selection(token: str):
    try:
        if not token:
            return False
        headers = {"Authorization": token, "User-Agent": "Mozilla/5.0"}
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://discord.com/api/v9/users/@me", headers=headers) as res:
                return res.status == 200
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return False

async def validate_all_accounts_concurrent_for_channel(accounts: list):
    if not accounts:
        return []

    validated_accounts = []
    for account in accounts:
        try:
            res = await validate_token_for_selection(account.get('token', ''))
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            res = False
        account_copy = account.copy()
        account_copy['_validation_status'] = 'valid' if res else 'invalid'
        validated_accounts.append(account_copy)

    return validated_accounts


async def get_channel_info(token: str, channel_id: str):
    try:
        headers = {"Authorization": token, "User-Agent": "Mozilla/5.0"}
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"https://discord.com/api/v9/channels/{channel_id}", headers=headers) as res:
                if res.status != 200:
                    return None
                channel_data = await res.json(content_type=None)
            info = {
                "channel_id": channel_data['id'],
                "channel_name": channel_data.get('name', 'Unknown Channel'),
                "server_name": "DM",
                "profile_server": None,
                "guild_id": None,
                "rate_limit_per_user": channel_data.get('rate_limit_per_user', 0)
            }
            guild_id = channel_data.get('guild_id')
            if guild_id:
                info["guild_id"] = guild_id
                async with session.get(f"https://discord.com/api/v9/guilds/{guild_id}", headers=headers) as guild_res:
                    if guild_res.status == 200:
                        guild_data = await guild_res.json(content_type=None)
                        info["server_name"] = guild_data.get('name', 'Unknown Server')
                        icon_hash = guild_data.get('icon')
                        if icon_hash:
                            info["profile_server"] = f"https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.png"
            return info
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError):
        return None

class StartAllModal(ui.Modal, title="Delay Start"):
    delay_input = ui.TextInput(label="Delay between starts ex : 2s,3s", placeholder="Use '-' for randoming ex 2s-5s", required=True)

    def __init__(self, original_interaction: discord.Interaction, view):
        super().__init__()
        self.original_interaction = original_interaction
        self.parent_view = view

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        delay_parts = self.delay_input.value.replace(' ', '').split('-')
        if not (1 <= len(delay_parts) <= 2):
            await interaction.followup.send("<:exclamation:1426164277638594680> Invalid delay format. Please use a single value ex 2s,3s or a range ex ; 2s-5s.", ephemeral=True)
            return

        try:
            min_delay = parse_duration_to_seconds(delay_parts[0])
            max_delay = parse_duration_to_seconds(delay_parts[-1])
            if min_delay < 2 or max_delay < min_delay:
                raise ValueError("<:exclamation:1426164277638594680> Delay must be at least 2 seconds.")
        except (ValueError, IndexError):
            await interaction.followup.send("<:exclamation:1426164277638594680> Invalid delay value. Delay must be at least 2 seconds ex 2s / 2s-5s.", ephemeral=True)
            return

        async def staggered_start():
            manager = get_autopost_manager()
            db = get_db()
            user_config = await run_db(db.get_user, interaction.user.id)
            account = next((acc for acc in user_config['accounts'] if acc['user_id'] == self.parent_view.account_user_id), None)
            if not account:
                return

            channels_to_start = [ch for ch in account.get('channels', []) if not ch.get('is_active', False)]

            if not channels_to_start:
                return

            channel_ids_to_activate = [ch['channel_id'] for ch in channels_to_start]
            for ch in account.get('channels', []):
                if ch.get('channel_id') in channel_ids_to_activate:
                    ch['is_active'] = True
                    ch['last_uptime'] = 0
            sanitized_user = _prepare_user_config_for_storage(user_config)
            await run_db(db.add_user, sanitized_user)

            self.parent_view.update_view_components(user_config)
            embed = self.parent_view.create_embed()
            await self.parent_view.edit_host_message(embed=embed, view=self.parent_view)

            fresh_user_config = await run_db(db.get_user, interaction.user.id)
            fresh_account = next((acc for acc in fresh_user_config['accounts'] if acc['user_id'] == self.parent_view.account_user_id), None)
            if not fresh_account:
                return

            channel_map = {ch['channel_id']: ch for ch in fresh_account['channels']}

            for channel_id in channel_ids_to_activate:
                channel_data = channel_map.get(channel_id)
                if channel_data and channel_data.get('is_active'):
                    await manager.add_task(
                        AutoPostTask(fresh_user_config['user_id'], fresh_account, channel_data)
                    )
                    delay = random.randint(min_delay, max_delay)
                    await asyncio.sleep(delay)

        asyncio.create_task(staggered_start())


class ChannelAddModal(ui.Modal, title="Channel Addition"):
    def __init__(self, account: dict):
        super().__init__()
        self.account = account

        max_len = 4000 if account.get('is_nitro') else 2500

        self.add_item(ui.TextInput(label="Channel ID", placeholder="Enter the Channel ID", required=True))
        self.add_item(ui.TextInput(label="Delay", placeholder="Use Format s m h, if empety auto use slowmode channel", required=False))
        self.add_item(ui.TextInput(label="Random Delay", placeholder="Use for randoming delay post, use comma for separate", required=False))
        self.add_item(ui.TextInput(label="Message", placeholder="Enter the Auto Post text", style=discord.TextStyle.paragraph, required=True, max_length=max_len))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        db = get_db()

        channel_id = self.children[0].value
        channel_info = await get_channel_info(self.account['token'], channel_id)
        if not channel_info:
            await interaction.followup.send("<:exclamation:1426164277638594680> Sorry, the account does not have permission to access this channel. Please ensure the account is in the server and can view the channel.", ephemeral=True)
            return

        user_config = await run_db(db.get_user, interaction.user.id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data.", ephemeral=True)
            return
        target_account = next((acc for acc in user_config['accounts'] if acc['user_id'] == self.account['user_id']), None)
        if not target_account:
            await interaction.followup.send("<:exclamation:1426164277638594680> Account not found.", ephemeral=True)
            return

        if any(ch['channel_id'] == channel_id for ch in target_account.get('channels', [])):
            await interaction.followup.send("<:exclamation:1426164277638594680> This channel is already configured for this account.", ephemeral=True)
            return

        delay_input = self.children[1].value
        final_delay = delay_input if delay_input else format_seconds_to_duration(channel_info.get('rate_limit_per_user', 120))
        if not delay_input and channel_info.get('rate_limit_per_user', 0) == 0:
            final_delay = "2m"

        embed = discord.Embed(
            title="<:server:1426173792144724048> Channel Successfully Added",
            description=f"Channel <#{channel_id}> has been successfully added to this account.",
            color=interaction.client.config['global_color'],
            timestamp=datetime.now(timezone.utc)
        )
        if channel_info.get('profile_server'):
            embed.set_thumbnail(url=channel_info['profile_server'])

        details = (
            f"**<:dot:1426148484146270269> Channel Location**: <#{channel_info['channel_id']}>\n"
            f"<:dot:1426148484146270269> **Server Name**: {channel_info['server_name']}\n"
            f"**<:dot:1426148484146270269> Channel ID**: {channel_info['channel_id']}\n"
            f"**<:dot:1426148484146270269> Delay**: {final_delay}"
        )
        if self.children[2].value and self.children[2].value != 'none':
            details += f"\n**<:dot:1426148484146270269> Random Delay**: {self.children[2].value}"

        msg_preview = (self.children[3].value[:100] + '...') if len(self.children[3].value) > 100 else self.children[3].value
        details += f"\n**<:dot:1426148484146270269> Message**:\n```\n{msg_preview}\n```"

        embed.add_field(name="Channel Config  Details <:delete:1426170386734911578>", value=details, inline=False)
        embed.set_footer(text=interaction.client.config.get('global_footer', ''), icon_url=interaction.client.config.get('global_footer_icon', ''))
        img_url = interaction.client.config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)

        new_channel = {
            **channel_info,
            "teks": self.children[3].value,
            "delay": final_delay,
            "delay_random": self.children[2].value or "none",
            "is_active": False,
            "sent_count": 0,
            "last_uptime": 0
        }

        target_account.setdefault('channels', []).append(new_channel)
        sanitized_user = _prepare_user_config_for_storage(user_config)
        await run_db(db.add_user, sanitized_user)
        await interaction.followup.send(embed=embed, ephemeral=True)

class ChannelDashboardView(ui.View):
    def __init__(self, account: dict):
        super().__init__(timeout=180)
        self.account = account

    @ui.button(label="Add Channel", style=discord.ButtonStyle.primary, emoji="<:server:1426173792144724048>")
    async def add_channel(self, interaction: discord.Interaction, button: ui.Button):
        if interaction.response.is_done():
            return
        try:
            await interaction.response.send_modal(ChannelAddModal(self.account))
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("<:exclamation:1426164277638594680> Unable to open the channel configuration modal right now. Please try again.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

class ChannelEditModal(ui.Modal, title="Edit Channel Configuration"):
    def __init__(self, account: dict, channel: dict, view):
        super().__init__()
        self.account = account
        self.channel = channel
        self.parent_view = view
        max_len = 4000 if account.get('is_nitro') else 2500

        self.add_item(ui.TextInput(label="Channel ID", default=channel.get('channel_id'), required=True))
        self.add_item(ui.TextInput(label="Delay", default=channel.get('delay'), placeholder="If empety auto use slowmode channel", required=False))
        self.add_item(ui.TextInput(label="Random Delay", default=channel.get('delay_random'), placeholder="Use with comma separate ex 2m,3h", required=False))
        self.add_item(ui.TextInput(label="Message", default=channel.get('teks'), style=discord.TextStyle.paragraph, max_length=max_len, required=True))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data.", ephemeral=True)
            return
        account = next((acc for acc in user_config['accounts'] if acc['user_id'] == self.account['user_id']), None)
        if not account:
            await interaction.followup.send("<:exclamation:1426164277638594680> Error: Account not found.", ephemeral=True)
            return

        channel_to_edit = next((ch for ch in account.get('channels', []) if ch['channel_id'] == self.channel['channel_id']), None)
        if not channel_to_edit:
            await interaction.followup.send("<:exclamation:1426164277638594680> Error: Channel configuration not found.", ephemeral=True)
            return

        manager = get_autopost_manager()
        if manager:
            try:
                await manager.remove_task(f"{account['token']}_{channel_to_edit['channel_id']}")
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
        channel_to_edit['is_active'] = False

        new_channel_id = self.children[0].value.strip()
        new_info = await get_channel_info(account['token'], new_channel_id)

        if not new_info:
            sanitized_user = _prepare_user_config_for_storage(user_config)
            await run_db(db.add_user, sanitized_user)
            try:
                await interaction.followup.send(content="<:exclamation:1426164277638594680> Sorry, the account cannot access the new channel ID. The edit has been cancelled.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
            embed = self.parent_view.create_embed()
            await interaction.edit_original_response(content=None, embed=embed, view=self.parent_view)
            return

        delay_input = self.children[1].value.strip()
        final_delay = delay_input
        if not final_delay:
            slowmode = new_info.get('rate_limit_per_user', 0)
            final_delay = format_seconds_to_duration(slowmode if slowmode > 0 else 120)

        channel_to_edit.update({
            **new_info,
            "teks": self.children[3].value,
            "delay": final_delay,
            "delay_random": self.children[2].value.strip() or "none",
        })
        channel_to_edit.pop('attachments', None)

        sanitized_user = _prepare_user_config_for_storage(user_config)
        await run_db(db.add_user, sanitized_user)

        parent_view = self.parent_view
        parent_view.channel_id = new_channel_id

        parent_view.refresh_data(user_config)
        parent_view.update_buttons()

        embed = parent_view.create_embed()
        await interaction.edit_original_response(embed=embed, view=parent_view)
        try:
            ctrl_view = parent_view.parent_view
            ctrl_view.update_view_components(user_config)
            ctrl_embed = ctrl_view.create_embed()
            await ctrl_view.edit_host_message(embed=ctrl_embed, view=ctrl_view)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

class ChannelControlView(ui.View):
    def __init__(self, bot, original_interaction: discord.Interaction, user_id, account_user_id, initial_user_config: dict | None = None):
        super().__init__(timeout=300)
        self.bot = bot
        self.original_interaction = original_interaction
        self.user_id = user_id
        self.account_user_id = account_user_id
        self.current_page = 0
        self.items_per_page = 25
        self._user_config: dict | None = initial_user_config
        self.message = None
        self.update_view_components(initial_user_config)

    def attach_message(self, message: discord.Message | None):
        self.message = message

    async def edit_host_message(self, *, embed=None, view=None):
        if self.message:
            try:
                await self.message.edit(embed=embed, view=view)
                return
            except discord.NotFound:
                pass
        try:
            await self.original_interaction.edit_original_response(embed=embed, view=view)
        except (discord.NotFound, discord.InteractionResponded):
            pass

    def update_view_components(self, user_config: dict | None = None):
        if user_config is not None:
            self._user_config = user_config
        self.clear_items()
        user_config = self._user_config
        account = None
        if user_config:
            account = next((acc for acc in user_config.get('accounts', []) if acc['user_id'] == self.account_user_id), None)
        channels = account.get('channels', []) if account else []
        
        start_index = self.current_page * self.items_per_page
        end_index = start_index + self.items_per_page
        channels_on_page = channels[start_index:end_index]

        if channels:
            options = []
            for idx, ch in enumerate(channels_on_page):
                abs_index = start_index + idx
                options.append(discord.SelectOption(
                    label=f"#{ch['channel_name']}",
                    value=f"chidx:{abs_index}",
                    description=f"Status: {'ON' if ch.get('is_active') else 'OFF'}",
                    emoji="<:green:1426180913083191377>" if ch.get('is_active') else "<:red:1426180916446761031>"
                ))

            total_pages = (len(channels) + self.items_per_page - 1) // self.items_per_page
            placeholder = (
                f"Select Channel Here {self.current_page + 1}/{total_pages}"
                if total_pages > 1 else "Select Channel Here"
            )
            select = ui.Select(placeholder=placeholder, options=options)
            select.callback = self.on_channel_select
            self.add_item(select)

        self.add_item(ui.Button(label="Start All", style=discord.ButtonStyle.success, emoji="<:start:1426170397581250570>"))
        self.children[-1].callback = self.on_start_all
        self.add_item(ui.Button(label="Stop All", style=discord.ButtonStyle.danger, emoji="<:stop:1426170394792169502>"))
        self.children[-1].callback = self.on_stop_all

        if len(channels) > self.items_per_page:
            prev_btn = ui.Button(label="Previous", emoji="<:left:1426148440974299209>", disabled=self.current_page == 0)
            prev_btn.callback = self.prev_page
            self.add_item(prev_btn)
            next_btn = ui.Button(label="Next", emoji="<:right:1426148437836955658>", disabled=end_index >= len(channels))
            next_btn.callback = self.next_page
            self.add_item(next_btn)

    def create_embed(self):
        embed = discord.Embed(
            title="<:server:1426173792144724048> Channel Control & Manager",
            description="Use the `Start All` & `Stop All` buttons to manage all channels. Select a channel from the list to manage it individually.",
            color=self.bot.config['global_color'],
            timestamp=datetime.now(timezone.utc)
        )
        embed.set_footer(text=self.bot.config.get('global_footer', ''), icon_url=self.bot.config.get('global_footer_icon', ''))
        img_url = self.bot.config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)
        return embed

    async def respond_with_manager(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        embed = self.create_embed()
        await interaction.edit_original_response(embed=embed, view=self)

    async def on_channel_select(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        sel = interaction.data['values'][0]
        ch_index = None
        if isinstance(sel, str) and sel.startswith('chidx:'):
            try:
                ch_index = int(sel.split(':', 1)[1])
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
                ch_index = None
        db = get_db()
        user_conf = await run_db(db.get_user, self.user_id)
        if not user_conf:
            await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data. Please try again.", ephemeral=True)
            return
        account = next((acc for acc in user_conf.get('accounts', []) if acc['user_id'] == self.account_user_id), None)
        if not account or ch_index is None or ch_index >= len(account.get('channels', [])):
            await interaction.followup.send("<:exclamation:1426164277638594680> Invalid selection.", ephemeral=True)
            return
        ch_obj = account['channels'][ch_index]
        view = ChannelManagerView(self.bot, self.user_id, self.account_user_id, ch_obj['channel_id'], self, channel_index=ch_index, user_config=user_conf)
        embed = view.create_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

    async def on_start_all(self, interaction: discord.Interaction):
        if interaction.response.is_done():
            return
        try:
            await interaction.response.send_modal(StartAllModal(interaction, self))
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("<:exclamation:1426164277638594680> Unable to open the bulk start modal. Please try again.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

    async def on_stop_all(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        db, manager = get_db(), get_autopost_manager()
        user_conf = await run_db(db.get_user, self.user_id)
        if not user_conf:
            await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data.", ephemeral=True)
            return
        account = next((acc for acc in user_conf['accounts'] if acc['user_id'] == self.account_user_id), None)

        if account:
            active_channels = [ch for ch in account['channels'] if ch.get('is_active')]
            for channel in active_channels:
                task_key = f"{account['token']}_{channel['channel_id']}"
                client = manager.get_task(task_key)
                if client:
                    try:
                        channel['last_uptime'] = int(time.monotonic() - client.start_time)
                    except Exception:
                        logger.exception('Unhandled exception in %s', __name__)
                        channel['last_uptime'] = 0
                channel['is_active'] = False
                try:
                    await manager.remove_task(task_key)
                except Exception as e:
                    logger.exception('Unhandled exception in %s', __name__)
            sanitized_user = _prepare_user_config_for_storage(user_conf)
            await run_db(db.add_user, sanitized_user)

        self.update_view_components(user_conf)
        embed = self.create_embed()
        await interaction.edit_original_response(embed=embed, view=self)
    
    async def prev_page(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        self.current_page -= 1
        self.update_view_components()
        await interaction.edit_original_response(view=self)

    async def next_page(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        self.current_page += 1
        self.update_view_components()
        await interaction.edit_original_response(view=self)

class ChannelManagerView(ui.View):
    def __init__(self, bot, user_id, account_user_id, channel_id, parent_view, channel_index: int | None = None, *, user_config: dict | None = None):
        super().__init__(timeout=300)
        self.bot = bot
        self.user_id = user_id
        self.account_user_id = account_user_id
        self.channel_id = channel_id
        self.parent_view = parent_view
        self.channel_index = channel_index
        self._user_config = user_config
        self.refresh_data(user_config)
        self.update_buttons()

    def refresh_data(self, user_config: dict | None = None):
        if user_config is not None:
            self._user_config = user_config
        user_config = self._user_config
        if not user_config:
            self.account = None
            self.channel = None
            return
        self.account = next((acc for acc in user_config.get('accounts', []) if acc['user_id'] == self.account_user_id), None)
        if self.account is None:
            self.channel = None
            return
        channels = self.account.get('channels', [])
        if isinstance(self.channel_index, int) and 0 <= self.channel_index < len(channels):
            self.channel = channels[self.channel_index]
            self.channel_id = self.channel.get('channel_id', self.channel_id)
        else:
            self.channel = next((ch for ch in channels if ch.get('channel_id') == self.channel_id), None)

    def update_buttons(self):
        self.clear_items()
        if not self.channel: return
        is_active = self.channel.get('is_active', False)
        
        action_btn = ui.Button(label="Stop", style=discord.ButtonStyle.danger, emoji="<:stop:1426170394792169502>") if is_active else ui.Button(label="Start", style=discord.ButtonStyle.success, emoji="<:start:1426170397581250570>")
        action_btn.callback = self.stop_channel if is_active else self.start_channel
        self.add_item(action_btn)

        edit_btn = ui.Button(label="Edit", style=discord.ButtonStyle.primary, emoji="<:edit:1426170391218487349>")
        edit_btn.callback = self.edit_channel
        self.add_item(edit_btn)
        
        delete_btn = ui.Button(label="Delete", style=discord.ButtonStyle.danger, emoji="<:trash:1426173796796203029>")
        delete_btn.callback = self.delete_channel
        self.add_item(delete_btn)

    async def _update_db_and_respond(self, interaction: discord.Interaction, user_config):
        sanitized_user = _prepare_user_config_for_storage(user_config)
        await run_db(get_db().add_user, sanitized_user)
        self.refresh_data(user_config)
        self.update_buttons()
        embed = self.create_embed()
        await interaction.edit_original_response(embed=embed, view=self)

    def create_embed(self):
        self.refresh_data()
        if not self.channel: return discord.Embed(title="<:exclamation:1426164277638594680> Error", description="Channel not found.", color=discord.Color.red())
        
        embed = discord.Embed(title="<:server:1426173792144724048> Channel Manager", description=f"Control & Manager for channel <#{self.channel['channel_id']}>", color=self.bot.config['global_color'], timestamp=datetime.now(timezone.utc))
        if self.channel.get('profile_server'): embed.set_thumbnail(url=self.channel['profile_server'])

        status_value = "<:green:1426180913083191377> Online" if self.channel.get('is_active') else "<:red:1426180916446761031> Offline"
        uptime_value = format_uptime(self.channel.get('last_uptime', 0))
        embed.add_field(name="Channel Status <:clover:1426170416606478437>", value=f"**<:dot:1426148484146270269> Status**: {status_value}\n**<:dot:1426148484146270269> Sent Count**: {self.channel.get('sent_count', 0)}\n**<:dot:1426148484146270269> Last Uptime**: {uptime_value}", inline=False)

        details = (
            f"**<:dot:1426148484146270269> Channel Location**: <#{self.channel['channel_id']}>\n"
            f"**<:dot:1426148484146270269> Server Name**: {self.channel['server_name']}\n"
            f"**<:dot:1426148484146270269> Channel ID**: {self.channel['channel_id']}\n"
            f"**<:dot:1426148484146270269> Delay**: {self.channel['delay']}"
        )
        if self.channel.get('delay_random') not in [None, 'none', '']: 
            details += f"\n**<:dot:1426148484146270269> Random Delay**: {self.channel['delay_random']}"
        msg_preview = (self.channel['teks'][:100] + '...') if len(self.channel['teks']) > 100 else self.channel['teks']
        details += f"\n**<:dot:1426148484146270269> Message**:\n```\n{msg_preview}\n```"
        embed.add_field(name="Detail Config <:scroll:1426148423157022782>", value=details, inline=False)
        embed.set_footer(text=self.bot.config.get('global_footer', ''), icon_url=self.bot.config.get('global_footer_icon', ''))
        img_url = self.bot.config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)
        return embed

    async def start_channel(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        db = get_db()
        user_config = await run_db(db.get_user, self.user_id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data.", ephemeral=True)
            return
        account = next(acc for acc in user_config['accounts'] if acc['user_id'] == self.account_user_id)
        if isinstance(self.channel_index, int) and 0 <= self.channel_index < len(account['channels']):
            channel = account['channels'][self.channel_index]
            self.channel_id = channel['channel_id']
        else:
            channel = next(ch for ch in account['channels'] if ch['channel_id'] == self.channel_id)
        channel['is_active'] = True
        channel['last_uptime'] = 0
        key = f"{account['token']}_{self.channel_id}"
        await get_autopost_manager().add_task(
            AutoPostTask(user_config['user_id'], account, channel)
        )
        await self._update_db_and_respond(interaction, user_config)
        try:
            self.parent_view.update_view_components(user_config)
            ctrl_embed = self.parent_view.create_embed()
            await self.parent_view.edit_host_message(embed=ctrl_embed, view=self.parent_view)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

    async def stop_channel(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        db = get_db()
        user_config = await run_db(db.get_user, self.user_id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data.", ephemeral=True)
            return
        account = next(acc for acc in user_config['accounts'] if acc['user_id'] == self.account_user_id)
        if isinstance(self.channel_index, int) and 0 <= self.channel_index < len(account['channels']):
            channel = account['channels'][self.channel_index]
            self.channel_id = channel['channel_id']
        else:
            channel = next(ch for ch in account['channels'] if ch['channel_id'] == self.channel_id)
        
        task_key = f"{account['token']}_{self.channel_id}"
        manager = get_autopost_manager()
        try:
            client = manager.get_task(task_key)
        except Exception as e:
            logger.exception('Unhandled exception in %s', __name__)
            client = None
            pass
        if client:
            try:
                channel['last_uptime'] = int(time.monotonic() - client.start_time)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
                channel['last_uptime'] = 0
        channel['is_active'] = False
        try:
            await manager.remove_task(task_key)
        except Exception as e:
            logger.exception('Unhandled exception in %s', __name__)
        await self._update_db_and_respond(interaction, user_config)
        try:
            self.parent_view.update_view_components(user_config)
            ctrl_embed = self.parent_view.create_embed()
            await self.parent_view.edit_host_message(embed=ctrl_embed, view=self.parent_view)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
    
    async def edit_channel(self, interaction: discord.Interaction):
        self.refresh_data()
        if not self.channel: return
        if interaction.response.is_done():
            return
        try:
            await interaction.response.send_modal(ChannelEditModal(self.account, self.channel, self))
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("<:exclamation:1426164277638594680> Unable to open the edit modal right now. Please try again.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

    async def delete_channel(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        db = get_db()
        user_config = await run_db(db.get_user, self.user_id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data.", ephemeral=True)
            return
        account = next((acc for acc in user_config['accounts'] if acc['user_id'] == self.account_user_id), None)

        if account:
            key = f"{account['token']}_{self.channel_id}"
            try:
                await get_autopost_manager().remove_task(key)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

            if isinstance(self.channel_index, int) and 0 <= self.channel_index < len(account.get('channels', [])):
                del account['channels'][self.channel_index]
            else:
                account['channels'] = [ch for ch in account.get('channels', []) if ch.get('channel_id') != self.channel_id]
            sanitized_user = _prepare_user_config_for_storage(user_config)
            await run_db(get_db().add_user, sanitized_user)

        self.parent_view.update_view_components(user_config)
        try:
            embed = discord.Embed(
                title="<:trash:1426173796796203029> Channel Deleted",
                description=f"Channel <#{self.channel_id}> successfully deleted and stopped.",
                color=interaction.client.config['global_color'],
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=interaction.client.config.get('global_footer', ''), icon_url=interaction.client.config.get('global_footer_icon', ''))
            img_url = interaction.client.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)
            await interaction.edit_original_response(embed=embed, view=None)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.edit_original_response(content=f"Channel <#{self.channel_id}> successfully deleted and stopped.", embed=None, view=None)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
        try:
            ctrl_embed = self.parent_view.create_embed()
            await self.parent_view.edit_host_message(embed=ctrl_embed, view=self.parent_view)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

class CopyToModal(ui.Modal, title="Copy Channel Configuration"):
    def __init__(self, source_account: dict, all_accounts: list, db):
        super().__init__()
        self.source_account = source_account
        self.all_accounts = all_accounts
        self.db = db

        other_accounts = [acc for acc in all_accounts if acc['user_id'] != source_account['user_id']]
        default_usernames = ",".join([acc['username'] for acc in other_accounts])

        self.destination_accounts_input = ui.TextInput(
            label="Destination Account Usernames",
            placeholder="Enter usernames, separated by commas (,)",
            default=default_usernames,
            style=discord.TextStyle.paragraph
        )
        self.add_item(self.destination_accounts_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        user_config = await run_db(self.db.get_user, interaction.user.id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> An error occurred while fetching your data.", ephemeral=True)
            return

        source_channels_data = next((acc.get('channels', []) for acc in user_config.get('accounts', []) if acc['user_id'] == self.source_account['user_id']), None)

        if source_channels_data is None:
            await interaction.followup.send("<:exclamation:1426164277638594680> Could not find the source account's channel configuration.", ephemeral=True)
            return

        source_channels = copy.deepcopy(source_channels_data)

        target_usernames = {name.strip() for name in self.destination_accounts_input.value.split(',') if name.strip()}
        updated_usernames = []

        for account in user_config.get('accounts', []):
            if account['username'] in target_usernames and account['user_id'] != self.source_account['user_id']:
                dest_channels = account.setdefault('channels', [])
                existing_ids = {ch.get('channel_id') for ch in dest_channels}
                appended_any = False
                for source_channel in source_channels:
                    channel_id = source_channel.get('channel_id')
                    if not channel_id or channel_id in existing_ids:
                        continue
                    dest_channels.append(copy.deepcopy(source_channel))
                    existing_ids.add(channel_id)
                    appended_any = True
                if appended_any:
                    updated_usernames.append(f"`{account['username']}`")

        if not updated_usernames:
            await interaction.followup.send("<:exclamation:1426164277638594680> No valid destination accounts were specified or found.", ephemeral=True)
            return

        sanitized_user = _prepare_user_config_for_storage(user_config)
        await run_db(self.db.add_user, sanitized_user)

        updated_list_str = ", ".join(updated_usernames)
        await interaction.followup.send(
            f"<:clover:1426170416606478437> The channel configuration from ``{self.source_account['username']}`` has been successfully copied to the following account(s): {updated_list_str}.",
            ephemeral=True
        )

class AccountSelectionViewForChannel(ui.View):
    def __init__(self, bot, original_interaction: discord.Interaction, action: str, accounts: list):
        super().__init__(timeout=180)
        self.bot = bot
        self.original_interaction = original_interaction
        self.action = action
        self.user_id = str(original_interaction.user.id)
        self.accounts = accounts if accounts else []
        self.current_page = 0
        self.items_per_page = 25

    @classmethod
    async def create(cls, bot, interaction: discord.Interaction, action: str):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)

            db = get_db()
            user_config = await run_db(db.get_user, interaction.user.id)
            accounts = user_config.get('accounts', []) if user_config else []

            if not accounts:
                await interaction.followup.send("<:exclamation:1426164277638594680> No Account Configured Using Command `/account` & Use Button `Add Account` to added the account", ephemeral=True)
                return None

            validated_accounts = await validate_all_accounts_concurrent_for_channel(accounts)
            valid_count = sum(1 for acc in validated_accounts if acc.get('_validation_status') == 'valid')
            invalid_count = len(validated_accounts) - valid_count

            view = cls(bot, interaction, action, validated_accounts)
            await view.update_components()

            title = f"<:server:1426173792144724048> Channel {action.capitalize()}"
            description = f"Please select an account to {action} channels for."
            if action == 'copy':
                title = "<:user:1426148393092386896> Select Source Account"
                description = "Please select the source account to copy the channel configuration from."

            embed = discord.Embed(title=title, description=description, color=bot.config['global_color'], timestamp=datetime.now(timezone.utc))
            embed.set_footer(text=bot.config.get('global_footer', ''), icon_url=bot.config.get('global_footer_icon'))
            img_url = bot.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)
            await interaction.edit_original_response(embed=embed, view=view)
            return view

        except Exception as e:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("An error occurred while processing your request.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
                return None

    async def update_components(self):
        self.clear_items()

        if not self.accounts or not isinstance(self.accounts, list):
            self.accounts = []

        if not self.accounts:
            self.add_item(ui.Select(placeholder="No accounts to select", options=[discord.SelectOption(label="No accounts available", value="none", description="No accounts found", emoji="<:exclamation:1426164277638594680>")], disabled=True))
            return

        start_index = self.current_page * self.items_per_page
        end_index = start_index + self.items_per_page
        accounts_on_page = self.accounts[start_index:end_index]

        options = []
        for idx, acc in enumerate(accounts_on_page):
            abs_index = start_index + idx
            options.append(
                discord.SelectOption(
                    label=acc['username'],
                    value=f"accidx:{abs_index}",
                    emoji="<:green:1426180913083191377>" if acc.get('_validation_status') == 'valid' else "<:red:1426180916446761031>",
                    description=f"Status: {'Valid' if acc.get('_validation_status') == 'valid' else 'Invalid'} | ID: {acc['user_id']}"
                )
            )

        if not options:
            self.add_item(ui.Select(placeholder="No accounts on this page", options=[discord.SelectOption(label="No accounts on this page", value="none", description="No accounts available on this page", emoji="<:exclamation:1426164277638594680>")], disabled=True))
            return

        total_pages = (len(self.accounts) + self.items_per_page - 1) // self.items_per_page
        placeholder = (
            f"Select Account Here {self.current_page + 1}/{total_pages}"
            if total_pages > 1
            else "Select Account Here"
        )

        select = ui.Select(placeholder=placeholder, options=options)
        select.callback = self.handle_selection
        self.add_item(select)

        if len(self.accounts) > self.items_per_page:
            prev_button = ui.Button(label="Previous", emoji="<:left:1426148440974299209>", style=discord.ButtonStyle.secondary, disabled=self.current_page == 0)
            prev_button.callback = self.prev_page
            self.add_item(prev_button)
            
            next_button = ui.Button(label="Next", emoji="<:right:1426148437836955658>", style=discord.ButtonStyle.secondary, disabled=end_index >= len(self.accounts))
            next_button.callback = self.next_page
            self.add_item(next_button)

    async def prev_page(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        self.current_page -= 1
        await self.update_components()
        await interaction.edit_original_response(view=self)

    async def next_page(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        self.current_page += 1
        await self.update_components()
        await interaction.edit_original_response(view=self)

    async def handle_selection(self, interaction: discord.Interaction):
        selected_value = interaction.data['values'][0]
        if selected_value == "none":
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            await interaction.followup.send("<:exclamation:1426164277638594680> No valid account selected.", ephemeral=True)
            return

        acc_index = None
        if isinstance(selected_value, str) and selected_value.startswith('accidx:'):
            try:
                acc_index = int(selected_value.split(':', 1)[1])
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
                acc_index = None
        selected_account = None
        if acc_index is not None and 0 <= acc_index < len(self.accounts):
            selected_account = self.accounts[acc_index]
        else:
            selected_account = next((acc for acc in self.accounts if acc.get('user_id') == selected_value), None)
        if not selected_account:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            await interaction.followup.send("<:exclamation:1426164277638594680> The selected account could not be found.", ephemeral=True)
            return

        if selected_account.get('_validation_status') == 'invalid':
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            await interaction.followup.send("<:exclamation:1426164277638594680> Sorry, the selected account's token is invalid. Please update it using the `/account` command.", ephemeral=True)
            return

        if self.action == 'copy':
            if not selected_account.get('channels'):
                if not interaction.response.is_done():
                    await interaction.response.defer(ephemeral=True)
                await interaction.followup.send("<:exclamation:1426164277638594680> The selected account has no channels configured.", ephemeral=True)
                return
            if interaction.response.is_done():
                return
            try:
                await interaction.response.send_modal(CopyToModal(selected_account, self.accounts, get_db()))
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
                try:
                    await interaction.followup.send("<:exclamation:1426164277638594680> Unable to open the copy modal. Please try again.", ephemeral=True)
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)

        elif self.action == 'add':
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            embed = discord.Embed(title="<:server:1426173792144724048> Add Channel", description="To add a channel for auto-posting, please use the 'Add Channel' button below.", color=interaction.client.config['global_color'], timestamp=datetime.now(timezone.utc))
            embed.set_footer(text=interaction.client.config.get('global_footer', ''), icon_url=interaction.client.config.get('global_footer_icon', ''))
            img_url = interaction.client.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)
            await interaction.edit_original_response(embed=embed, view=ChannelDashboardView(selected_account))

        elif self.action == 'control':
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            channels = selected_account.get('channels') or []
            if not channels:
                await interaction.followup.send(
                    "<:exclamation:1426164277638594680> The selected account has no channels configured. Please add channels first using `/channel add`.",
                    ephemeral=True
                )
                return
            db = get_db()
            user_config = await run_db(db.get_user, self.user_id)
            if not user_config:
                await interaction.followup.send("<:exclamation:1426164277638594680> Unable to load your account data.", ephemeral=True)
                return
            view = ChannelControlView(self.bot, interaction, str(interaction.user.id), selected_account['user_id'], initial_user_config=user_config)
            embed = view.create_embed()
            message = await interaction.followup.send(embed=embed, view=view, ephemeral=True, wait=True)
            view.attach_message(message)

class ChannelCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = get_db()
    
    channel_group = app_commands.Group(name="channel", description="Manage auto post channels")

    async def _check_user(self, interaction: discord.Interaction) -> bool:
        user_config = await ensure_active_subscription(
            interaction,
            require_accounts=True
        )
        return user_config is not None

    async def send_selection_menu(self, interaction: discord.Interaction, action: str):
        try:
            view = await AccountSelectionViewForChannel.create(self.bot, interaction, action)
            if view is None:
                return

        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "<:exclamation:1426164277638594680> **Error occurred while loading the menu. Please try again.**",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "<:exclamation:1426164277638594680> **Error occurred while loading the menu. Please try again.**",
                        ephemeral=True
                    )
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

    @channel_group.command(name="add", description="Configure a new channel for an account")
    async def add(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            if not await self._check_user(interaction):
                return
            await self.send_selection_menu(interaction, 'add')
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("<:exclamation:1426164277638594680> An error occurred while processing your request.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

    @channel_group.command(name="control", description="Control your auto post channels")
    async def control(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            if not await self._check_user(interaction):
                return
            await self.send_selection_menu(interaction, 'control')
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("<:exclamation:1426164277638594680> An error occurred while processing your request.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

    @channel_group.command(name="copy", description="Copy all channel configurations from one account to others")
    async def copy(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            if not await self._check_user(interaction):
                return
            await self.send_selection_menu(interaction, 'copy')
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("<:exclamation:1426164277638594680> An error occurred while processing your request.", ephemeral=True)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)

async def setup(bot):
    await bot.add_cog(ChannelCog(bot))
