import copy
import aiohttp
import discord
from discord import ui, app_commands
from discord.ext import commands
from datetime import datetime, timezone
import asyncio

from database import get_db, run_db
from handler.autoreply import get_autoreply_manager
from commands.utils import ensure_active_subscription

async def _fetch_account_identity(session: aiohttp.ClientSession, token: str):
    if not token:
        return None, None
    headers = {"Authorization": token, "User-Agent": "Mozilla/5.0"}
    try:
        async with session.get("https://discord.com/api/v9/users/@me", headers=headers) as res:
            if res.status == 200:
                data = await res.json(content_type=None)
                return data.get('username'), data.get('id')
    except aiohttp.ClientError:
        pass
    return None, None

async def validate_all_accounts_concurrent_for_autoreply(accounts: list):
    if not accounts:
        return []

    timeout = aiohttp.ClientTimeout(total=6)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [_fetch_account_identity(session, acc.get('token', '')) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    validated_accounts = []
    for account, res in zip(accounts, results):
        account_copy = copy.deepcopy(account)
        if isinstance(res, Exception) or not res or not res[0]:
            account_copy['_validation_status'] = 'invalid'
        else:
            account_copy['_validation_status'] = 'valid'
        validated_accounts.append(account_copy)

    return validated_accounts


class SettingsModal(ui.Modal, title='Auto Reply Settings'):
    def __init__(self, bot, user_id: str, account: dict, view):
        super().__init__(timeout=None)
        self.bot = bot
        self.db = get_db()
        self.user_id = user_id
        self.account = account
        self.parent_view = view

        autoreply_config = self.account.get('autoreply', {})
        max_length = 4000 if self.account.get('is_nitro') else 2000

        self.presence_input = ui.TextInput(
            label="Presence",
            placeholder="online / idle / dnd / invisible",
            default=autoreply_config.get('presence', 'online'),
            required=True
        )
        self.message_input = ui.TextInput(
            label="Message",
            style=discord.TextStyle.paragraph,
            placeholder="Enter the auto-reply message.",
            default=autoreply_config.get('message', ''),
            max_length=max_length,
            required=True
        )

        self.add_item(self.presence_input)
        self.add_item(self.message_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        presence = self.presence_input.value.lower()
        if presence not in ["online", "idle", "dnd", "invisible"]:
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Invalid presence value. Please use one of: `online`, `idle`, `dnd`, `invisible`.")
            return

        new_config = {
            "presence": presence,
            "message": self.message_input.value,
        }

        await run_db(self.db.update_account_autoreply_settings, self.user_id, self.account['token'], new_config)

        updated_user_config = await run_db(self.db.get_user, self.user_id)
        updated_account = next((acc for acc in updated_user_config['accounts'] if acc['token'] == self.account['token']), None)

        if updated_account:
            self.parent_view.account = updated_account
            embed = self.parent_view.create_embed()
            await interaction.edit_original_response(embed=embed, view=self.parent_view)
        else:
            await interaction.edit_original_response(content="Settings updated successfully.")

class AutoReplyControlView(ui.View):
    def __init__(self, bot, user_id: str, account: dict):
        super().__init__(timeout=180)
        self.bot = bot
        self.user_id = user_id
        self.account = account
        self.update_buttons()

    def create_embed(self) -> discord.Embed:
        config = self.bot.config
        autoreply_config = self.account.get('autoreply', {})
        status = "Online" if autoreply_config.get('isactive') else "Offline"
        
        embed = discord.Embed(
            title="Auto Reply Control & Setup <:selophone:1426164287360995389>",
            description="Manage and run your auto-reply settings.",
            color=config.get('global_color'),
            timestamp=datetime.now(timezone.utc)
        )
        if self.account.get('profile_url'):
            embed.set_thumbnail(url=self.account['profile_url'])

        def truncate(s: str, length: int) -> str:
            if not s: return "Not Set"
            return (s[:length] + '...') if len(s) > length else s

        value = f"**<:dot:1426148484146270269> Status**: {status}\n"
        value += f"**<:dot:1426148484146270269> Presence**: {autoreply_config.get('presence', 'Not Set').capitalize()}\n"

        msg = autoreply_config.get('message')
        if msg:
            value += f"**<:dot:1426148484146270269> Message**:\n```{truncate(msg, 50)}```\n"

        embed.add_field(name="Configuration & Status <:clover:1426170416606478437>", value=value, inline=False)
        embed.set_footer(text=config.get('global_footer', ''), icon_url=config.get('global_footer_icon', ''))
        img_url = config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)

        return embed

    def update_buttons(self):
        self.clear_items()

        is_active = self.account.get('autoreply', {}).get('isactive', False)

        if is_active:
            toggle_button = ui.Button(style=discord.ButtonStyle.danger, label="Stop", emoji="<:stop:1426170394792169502>")
        else:
            toggle_button = ui.Button(style=discord.ButtonStyle.success, label="Start", emoji="<:start:1426170397581250570>")
        toggle_button.callback = self.toggle_callback
        self.add_item(toggle_button)

        settings_button = ui.Button(style=discord.ButtonStyle.secondary, label="Settings", emoji="<:edit:1426170391218487349>")
        settings_button.callback = self.settings_callback
        self.add_item(settings_button)

        reset_button = ui.Button(style=discord.ButtonStyle.secondary, label="Reset Replied", emoji="<:delete:1426170386734911578>")
        reset_button.callback = self.reset_callback
        self.add_item(reset_button)

    async def settings_callback(self, interaction: discord.Interaction):
        modal = SettingsModal(self.bot, self.user_id, self.account, self)
        await interaction.response.send_modal(modal)
        
    async def reset_callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        db = get_db()
        await run_db(db.reset_replied_list_for_account, self.user_id, self.account['token'])
        await interaction.followup.send("<:green:1426180913083191377> Successfully cleared the replied user list for this account.", ephemeral=True)

    async def toggle_callback(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        manager = get_autoreply_manager()
        if not manager:
            await interaction.followup.send("<:exclamation:1426164277638594680> Autoreply manager is not available right now. Please try again in a moment.", ephemeral=True)
            return
        is_currently_active = self.account.get('autoreply', {}).get('isactive', False)
        desired_state = not is_currently_active
        action_success = True

        if is_currently_active:
            async def _stop_wrapper():
                try:
                    await manager.stop_task(self.user_id, self.account['token'])
                except Exception:
                    pass
            asyncio.create_task(_stop_wrapper())
        else:
            autoreply_conf = self.account.get('autoreply', {}) or {}
            if not autoreply_conf.get('message'):
                try:
                    await interaction.followup.send("<:exclamation:1426164277638594680> Pls configure auto reply config", ephemeral=True)
                except Exception:
                    pass
                return
            try:
                action_success = await manager.start_task(self.user_id, self.account)
            except Exception:
                action_success = False
            
            if not action_success:
                await interaction.followup.send("<:exclamation:1426164277638594680> Failed to start autoreply for this account. Please check the token or try again later.", ephemeral=True)
                return
        db = get_db()
        await run_db(db.update_autoreply_status, self.user_id, self.account['token'], desired_state)
        updated_user_config = await run_db(db.get_user, self.user_id)
        if updated_user_config:
            self.account = next((acc for acc in updated_user_config.get('accounts', []) if acc['token'] == self.account['token']), self.account)

        self.update_buttons()
        embed = self.create_embed()
        await interaction.edit_original_response(content=None, embed=embed, view=self)

class AccountSelectionViewForAutoReply(ui.View):
    def __init__(self, bot, original_interaction: discord.Interaction, accounts: list | None = None):
        super().__init__(timeout=180)
        self.bot = bot
        self.original_interaction = original_interaction
        self.user_id = str(original_interaction.user.id)
        self.accounts = accounts or []
        self.current_page = 0
        self.items_per_page = 25

    async def update_components(self):
        self.clear_items()

        if not self.accounts or not isinstance(self.accounts, list):
            self.accounts = []

        if len(self.accounts) == 0:
            placeholder_option = discord.SelectOption(
                label="No accounts available",
                value="none",
                description="No accounts found",
                emoji="<:exclamation:1426164277638594680>"
            )
            select = ui.Select(placeholder="No accounts to select", options=[placeholder_option], disabled=True)
            self.add_item(select)
            return

        start_index = self.current_page * self.items_per_page
        end_index = start_index + self.items_per_page
        accounts_on_page = self.accounts[start_index:end_index]

        options = []
        for idx, acc in enumerate(accounts_on_page):
            abs_index = start_index + idx
            validation_status = acc.get('_validation_status', 'unknown')
            if validation_status == 'valid':
                status_emoji = "<:green:1426180913083191377>"
                status_text = "Valid"
            else:
                status_emoji = "<:red:1426180916446761031>"
                status_text = "Invalid"

            description = f"Status: {status_text} | ID: {acc['user_id']}"
            options.append(discord.SelectOption(
                label=acc['username'],
                value=f"accidx:{abs_index}",
                description=description,
                emoji=status_emoji
            ))

        if not options:
            placeholder_option = discord.SelectOption(
                label="No accounts on this page",
                value="none",
                description="No accounts available on this page",
                emoji="<:exclamation:1426164277638594680>"
            )
            select = ui.Select(placeholder="No accounts on this page", options=[placeholder_option], disabled=True)
            self.add_item(select)
            return

        total_pages = (len(self.accounts) + self.items_per_page - 1) // self.items_per_page
        placeholder = "Select Account Here"
        if total_pages > 1:
            placeholder = f"Select Account Here {self.current_page + 1}/{total_pages}"

        select = ui.Select(placeholder=placeholder, options=options)
        select.callback = self.handle_selection
        self.add_item(select)

        if len(self.accounts) > self.items_per_page:
            prev_button = ui.Button(label="Previous", emoji="<:left:1426148440974299209>", style=discord.ButtonStyle.secondary, disabled=self.current_page == 0)
            prev_button.callback = self.prev_page
            next_button = ui.Button(label="Next", emoji="<:right:1426148437836955658>", style=discord.ButtonStyle.secondary, disabled=end_index >= len(self.accounts))
            next_button.callback = self.next_page
            self.add_item(prev_button)
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

    @classmethod
    async def create(cls, bot, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)

            db = get_db()
            user_config = await run_db(db.get_user, interaction.user.id)
            accounts = user_config.get('accounts', []) if user_config else []
            if not accounts:
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> No Account Configured Using Command `/account` & Use Button `Add Account` to added the account", embed=None, view=None)
                return
            
            validated_accounts = await validate_all_accounts_concurrent_for_autoreply(accounts)

            view = cls(bot, interaction, validated_accounts if validated_accounts else [])
            await view.update_components()
            embed = discord.Embed(
                title=" <:mannequin:1426148407646490624> Auto Reply Account Selection",
                description="Please select an account to configure its auto-reply settings.",
                color=bot.config['global_color'],
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(text=bot.config.get('global_footer', ''), icon_url=bot.config.get('global_footer_icon', ''))
            img_url = bot.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)
            await interaction.edit_original_response(embed=embed, view=view)

        except Exception:
            try:
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> An error occurred while processing your request.", embed=None, view=None)
            except (discord.NotFound, discord.InteractionResponded):
                pass

    async def handle_selection(self, interaction: discord.Interaction):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        sel = interaction.data['values'][0]

        if sel == "none":
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> No valid account selected.", embed=None, view=None)
            return

        acc_index = None
        if isinstance(sel, str) and sel.startswith('accidx:'):
            try:
                acc_index = int(sel.split(':', 1)[1])
            except Exception:
                acc_index = None
        selected_account_from_view = None
        if acc_index is not None and 0 <= acc_index < len(self.accounts):
            selected_account_from_view = self.accounts[acc_index]
        else:
            selected_account_from_view = next((acc for acc in self.accounts if acc.get('token') == sel), None)

        if not selected_account_from_view:
            await interaction.edit_original_response(content="Invalid selection.", embed=None, view=None)
            return
        token = selected_account_from_view['token']
        if selected_account_from_view and selected_account_from_view.get('_validation_status') == 'invalid':
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Sorry, the selected account's token is invalid. Please update it using the `/account` command.", embed=None, view=None)
            return

        db = get_db()
        user_config = await run_db(db.get_user, self.user_id)
        if not user_config:
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Unable to load your account data. Please try again.", embed=None, view=None)
            return
        account = next((acc for acc in user_config.get('accounts', []) if acc['token'] == token), None)

        if not account:
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Account not found. It might have been deleted.", embed=None, view=None)
            return

        view = AutoReplyControlView(self.bot, self.user_id, account)
        embed = view.create_embed()
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

class AutoReplyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_db()

    @app_commands.command(name="autoreply", description="Setup and manage auto-replies for your accounts.")
    async def autoreply(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            user_config = await ensure_active_subscription(
                interaction,
                require_accounts=True
            )
            if not user_config:
                return
            
            await AccountSelectionViewForAutoReply.create(self.bot, interaction)

        except Exception:
            try:
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> An error occurred while processing your request.", embed=None, view=None)
            except (discord.NotFound, discord.InteractionResponded):
                pass

async def setup(bot: commands.Bot):
    await bot.add_cog(AutoReplyCog(bot))
