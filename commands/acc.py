import discord
from discord import ui, app_commands
from discord.ext import commands
import aiohttp
from datetime import datetime, timezone
import asyncio

from database import get_db, run_db
from commands.utils import ensure_active_subscription
from handler.autopost import get_autopost_manager
from handler.autoreply import get_autoreply_manager
import logging

logger = logging.getLogger(__name__)
logger.handlers.clear()
logger.addHandler(logging.NullHandler())
logger.propagate = False


def _ensure_iso_datetime(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _sanitize_channels(channels):
    sanitized = []
    for channel in channels or []:
        if not isinstance(channel, dict):
            continue
        channel_copy = channel.copy()
        channel_copy.pop('attachments', None)
        sanitized.append(channel_copy)
    return sanitized


def _serialize_account_for_storage(account):
    if not isinstance(account, dict):
        return {}
    account_copy = account.copy()
    account_copy.pop('_validation_status', None)
    account_copy['created_at'] = _ensure_iso_datetime(account_copy.get('created_at'))
    added_at = account_copy.get('added_at') or datetime.now(timezone.utc)
    account_copy['added_at'] = _ensure_iso_datetime(added_at)
    account_copy['channels'] = _sanitize_channels(account_copy.get('channels'))
    return account_copy


def _prepare_accounts_for_storage(user_config):
    accounts = user_config.get('accounts', []) if isinstance(user_config, dict) else []
    user_config['accounts'] = [_serialize_account_for_storage(acc) for acc in accounts]
    return user_config


async def stop_account_automations(user_id: int | str, account: dict):
    if not account:
        return
    str_user_id = str(user_id)
    token = account.get('token')
    manager_reply = get_autoreply_manager()
    if manager_reply and token:
        try:
            await manager_reply.stop_task(str_user_id, token)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
    manager_post = get_autopost_manager()
    if manager_post and token:
        for channel in account.get('channels', []) or []:
            if isinstance(channel, dict):
                channel['is_active'] = False
                channel_id = channel.get('channel_id')
            else:
                channel_id = None
            if not channel_id:
                continue
            task_key = f"{token}_{channel_id}"
            try:
                await manager_post.remove_task(task_key)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
async def validate_token(token: str):
    try:
        headers = {"Authorization": token, "User-Agent": "Mozilla/5.0"}
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get("https://discord.com/api/v9/users/@me", headers=headers) as res:
                if res.status == 200:
                    user_data = await res.json(content_type=None)
                    user_id = user_data['id']
                    snowflake = int(user_id)
                    timestamp = ((snowflake >> 22) + 1420070400000) / 1000
                    created_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)

                    avatar_hash = user_data.get('avatar')
                    avatar_url = f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png?size=512" if avatar_hash else None
                    is_nitro = user_data.get('premium_type', 0) > 0

                    result = {
                        "token": token,
                        "user_id": user_id,
                        "username": user_data['username'],
                        "display_name": user_data.get('global_name') or user_data['username'],
                        "is_nitro": is_nitro,
                        "profile_url": avatar_url,
                        "created_at": created_at,
                        "channels": []
                    }
                    return result
        return None
    except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError):
        return None

async def validate_token_async(token: str):
    return await validate_token(token)

async def validate_all_accounts_concurrent(accounts: list):
    if not accounts:
        return []

    validated_accounts = []
    for acc in accounts:
        try:
            res = await validate_token_async(acc.get('token', ''))
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            res = None
        account_copy = acc.copy()
        if res is None:
            account_copy['_validation_status'] = 'invalid'
        else:
            account_copy['_validation_status'] = 'valid'
            account_copy.update(res)
        validated_accounts.append(account_copy)

    return validated_accounts

def create_success_embed(interaction: discord.Interaction, account_data: dict, title: str, description: str):
    embed = discord.Embed(
        title=title,
        description=description,
        color=interaction.client.config['global_color'],
        timestamp=datetime.now(timezone.utc)
    )
    if account_data.get('profile_url'):
        embed.set_thumbnail(url=account_data['profile_url'])

    created_at_ts = f"<t:{int(account_data['created_at'].timestamp())}:F>"

    details_value = (
        f"**<:dot:1426148484146270269> Display Name**: {account_data['display_name']}\n"
        f"**<:dot:1426148484146270269> Username**: {account_data['username']}\n"
        f"**<:dot:1426148484146270269> User ID**: {account_data['user_id']}\n"
        f"**<:dot:1426148484146270269> Nitro**: {'Yes' if account_data.get('is_nitro') else 'No'}\n"
        f"**<:dot:1426148484146270269> Date Created**: {created_at_ts}"
    )
    embed.add_field(name="Account Details <:user:1426148393092386896>", value=details_value, inline=False)

    embed.set_footer(
        text=interaction.client.config.get('global_footer', 'Bot Footer'),
        icon_url=interaction.client.config.get('global_footer_icon')
    )

    img_url = interaction.client.config.get('global_img_url')
    if img_url:
        embed.set_image(url=img_url)

    return embed

def create_main_account_embed(config: dict):
    embed = discord.Embed(
        title="<:user:1426148393092386896> Account Manager",
        description="Use the buttons below to manage your linked accounts.",
        color=config.get('global_color', 0x000000),
        timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(
        name="Instructions <:scroll:1426148423157022782>",
        value=(
            "**<:dot:1426148484146270269> Add Account**: Links a new Discord account using its token.\n"
            "**<:dot:1426148484146270269> Edit Account**: Updates the token for an existing linked account.\n"
            "**<:dot:1426148484146270269> Delete Account**: Unlinks a Discord account."
        ),
        inline=False
    )
    embed.set_footer(
        text=config.get('global_footer', 'Bot Footer'),
        icon_url=config.get('global_footer_icon')
    )
    img_url = config.get('global_img_url')
    if img_url:
        embed.set_image(url=img_url)
    return embed

class AddAccountModal(ui.Modal, title='Add New Account'):
    token_input = ui.TextInput(label='Discord Token', style=discord.TextStyle.short, placeholder='Enter your Discord token here', required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        token = self.token_input.value

        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> You are not registered to use this bot. Please contact the owner.", ephemeral=True)
            return
        limit = user_config.get('limit', -1) if user_config else -1
        if limit != -1 and len(user_config.get('accounts', [])) >= limit:
            await interaction.followup.send("<:exclamation:1426164277638594680> Sorry, you can't add more accounts limit reached.", ephemeral=True)
            return

        try:
            async with asyncio.timeout(60.0):
                account_data = await validate_token_async(token)
        except asyncio.TimeoutError:
            await interaction.followup.send("<:exclamation:1426164277638594680> Token validation timed out. Please try again.", ephemeral=True)
            return

        if not account_data:
            truncated_token = f"`{token[:15]}...`" if len(token) > 15 else f"`{token}`"
            await interaction.followup.send(
                (
                    f"<:exclamation:1426164277638594680> The token you entered `{truncated_token}` is invalid. "
                    "Please try again with a valid token. If you're trying to update the token "
                    "for an existing account, please use the 'Edit Account' button instead."
                ),
                ephemeral=True
            )
            return

        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> You are not registered to use this bot. Please contact the owner.", ephemeral=True)
            return
        if any(acc.get('user_id') == account_data['user_id'] for acc in user_config.get('accounts', [])):
            await interaction.followup.send(
                (
                    f"<:exclamation:1426164277638594680> Sorry, account <:user:1426148393092386896> `{account_data['username']}` is already registered in your profile. "
                    "Please use a different account. If you're trying to update this account's token, "
                    "use the 'Edit Account' button."
                ),
                ephemeral=True
            )
            return
        try:
            all_users = await run_db(db.get_all_users)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            all_users = []
        for u in all_users:
            for acc in u.get('accounts', []) or []:
                if acc.get('user_id') == account_data['user_id']:
                    await interaction.followup.send(
                        (
                            f"<:exclamation:1426164277638594680> Sorry, account <:user:1426148393092386896>`{account_data['username']}` is already registered. "
                            "Please use a different account. If you're trying to update this account's token, "
                            "use the 'Edit Account' button."
                        ),
                        ephemeral=True
                    )
                    return

        account_data['added_at'] = datetime.now(timezone.utc)
        user_config.setdefault('accounts', []).append(account_data)
        sanitized_user = _prepare_accounts_for_storage(user_config)
        await run_db(db.add_user, sanitized_user)

        success_embed = create_success_embed(
            interaction,
            account_data,
            title="<:exclamation:1426164277638594680> Account Added Successfully",
            description="The account has been linked to your profile."
        )
        try:
            await interaction.followup.send(embed=success_embed, ephemeral=True)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

class EditAccountModal(ui.Modal):
    def __init__(self, user_id_to_edit: str, original_interaction: discord.Interaction):
        super().__init__(title="Edit Account Token")
        self.user_id_to_edit = user_id_to_edit
        self.original_interaction = original_interaction
        self.new_token_input = ui.TextInput(label='New Discord Token', placeholder='Enter the new token', required=True)
        self.add_item(self.new_token_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        new_token = self.new_token_input.value

        try:
            async with asyncio.timeout(30.0):
                new_account_data = await validate_token_async(new_token)
        except asyncio.TimeoutError:
            await interaction.followup.send("<:exclamation:1426164277638594680> Token validation timed out. Please try again.", ephemeral=True)
            return

        if not new_account_data:
            truncated_token = f"`{new_token[:15]}...`" if len(new_token) > 15 else f"`{new_token}`"
            await interaction.followup.send(f"<:exclamation:1426164277638594680> The token you entered `{truncated_token}` is invalid. Please try again with a valid token.", ephemeral=True)
            return

        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        if not user_config:
            await interaction.followup.send("<:exclamation:1426164277638594680> You are not registered to use this bot. Please contact the owner.", ephemeral=True)
            return
        account_found = False
        for i, acc in enumerate(user_config.get('accounts', [])):
            if acc['user_id'] == self.user_id_to_edit:
                await stop_account_automations(interaction.user.id, acc)
                new_account_data['channels'] = acc.get('channels', [])
                new_account_data['added_at'] = acc.get('added_at', datetime.now(timezone.utc))
                user_config['accounts'][i] = new_account_data
                account_found = True
                break

        if account_found:
            sanitized_user = _prepare_accounts_for_storage(user_config)
            await run_db(db.add_user, sanitized_user)
            main_embed = create_main_account_embed(self.original_interaction.client.config)
            try:
                await self.original_interaction.edit_original_response(embed=main_embed, view=AccountDashboard(self.original_interaction.client.config))
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
        else:
            await interaction.followup.send("<:exclamation:1426164277638594680> Could not find the account to update.", ephemeral=True)

class DeleteConfirmModal(ui.Modal, title='Confirm Deletion'):
    confirmation = ui.TextInput(label="Confirmation", placeholder="Type 'confirm' to delete the account", default="confirm")

    def __init__(self, user_id_to_delete: str, original_interaction: discord.Interaction):
        super().__init__()
        self.user_id_to_delete = user_id_to_delete
        self.original_interaction = original_interaction

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        if self.confirmation.value.lower() != 'confirm':
            await interaction.followup.send("<:exclamation:1426164277638594680> Confirmation text did not match. Deletion cancelled.", ephemeral=True)
            return

        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)

        account_to_delete = next((acc for acc in user_config.get('accounts', []) if acc['user_id'] == self.user_id_to_delete), None)
        if not account_to_delete:
            await interaction.followup.send("<:exclamation:1426164277638594680> Could not find the account to delete.", ephemeral=True)
            return

        await stop_account_automations(interaction.user.id, account_to_delete)
        user_config['accounts'] = [acc for acc in user_config['accounts'] if acc['user_id'] != self.user_id_to_delete]
        sanitized_user = _prepare_accounts_for_storage(user_config)
        await run_db(db.add_user, sanitized_user)
        main_embed = create_main_account_embed(self.original_interaction.client.config)
        try:
            await self.original_interaction.edit_original_response(embed=main_embed, view=AccountDashboard(self.original_interaction.client.config))
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
        try:
            await interaction.followup.send(
                content=f"Account <:user:1426148393092386896> `{account_to_delete.get('username', 'unknown')}` has been deleted successfully.",
                ephemeral=True
            )
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

class AccountSelect(ui.Select):
    def __init__(self, options, action, placeholder):
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options)
        self.action = action

    async def callback(self, interaction: discord.Interaction):
        selected = self.values[0]
        if selected == "none":
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            await self.view.handle_selection(interaction, selected)
        else:
            await self.view.handle_selection(interaction, selected)

class AccountSelectionView(ui.View):
    def __init__(self, original_interaction: discord.Interaction, action: str, accounts: list | None = None):
        super().__init__(timeout=None)
        self.original_interaction = original_interaction
        self.action = action
        self.accounts = accounts or []
        self.current_page = 0
        self.items_per_page = 25
        self._main_embed = create_main_account_embed(self.original_interaction.client.config)

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

        self.add_item(AccountSelect(options, self.action, placeholder))

        if len(self.accounts) > self.items_per_page:
            prev_button = ui.Button(label="Previous", emoji="<:left:1426148440974299209>", style=discord.ButtonStyle.secondary, disabled=self.current_page == 0)
            prev_button.callback = self.prev_page

            next_button = ui.Button(label="Next", emoji="<:right:1426148437836955658>", style=discord.ButtonStyle.secondary, disabled=end_index >= len(self.accounts))
            next_button.callback = self.next_page

            self.add_item(prev_button)
            self.add_item(next_button)

        back_button = ui.Button(label="Back", emoji="<:left:1426148440974299209>", style=discord.ButtonStyle.secondary)
        back_button.callback = self.back_to_dashboard
        self.add_item(back_button)

    async def prev_page(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.current_page -= 1
        await self.update_components()
        await interaction.edit_original_response(view=self)

    async def next_page(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.current_page += 1
        await self.update_components()
        await interaction.edit_original_response(view=self)

    async def back_to_dashboard(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        dashboard_view = AccountDashboard(self.original_interaction.client.config)
        await self.original_interaction.edit_original_response(embed=self._main_embed, view=dashboard_view)
        self.stop()

    @classmethod
    async def create(cls, interaction: discord.Interaction, action: str):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)

            db = get_db()
            user_config = await run_db(db.get_user, interaction.user.id)
            accounts = user_config.get('accounts', []) if user_config else []

            if not accounts:
                await interaction.followup.send("You don't have any accounts yet.", ephemeral=True)
                return

            validated_accounts = await validate_all_accounts_concurrent(accounts)

            view = cls(interaction, action, validated_accounts if validated_accounts else [])
            await view.update_components()
            title_text = f"{action.capitalize()} Account <:user:1426148393092386896>"
            desc_text = f"All accounts have been validated. Please select an account to {action}."

            embed = discord.Embed(
                title=title_text,
                description=desc_text,
                color=interaction.client.config['global_color'],
                timestamp=datetime.now(timezone.utc)
            )
            embed.set_footer(
                text=interaction.client.config.get('global_footer', 'Bot Footer'),
                icon_url=interaction.client.config.get('global_footer_icon')
            )
            img_url = interaction.client.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)

            await interaction.edit_original_response(embed=embed, view=view)
            return view

        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.followup.send("<:exclamation:1426164277638594680> An error occurred while processing your request.", ephemeral=True)
            except (discord.NotFound, discord.InteractionResponded):
                pass
            return None

    async def handle_selection(self, interaction: discord.Interaction, selected_value: str):
        if selected_value == "none":
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True)
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> No valid account selected.", embed=None, view=None)
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
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Account not found.", embed=None, view=None)
            return

        if self.action == 'edit':
            await interaction.response.send_modal(EditAccountModal(selected_account['user_id'], self.original_interaction))
        elif self.action == 'delete':
            await interaction.response.send_modal(DeleteConfirmModal(selected_account['user_id'], self.original_interaction))

class AccountDashboard(ui.View):
    def __init__(self, bot_config):
        super().__init__(timeout=None)
        self.bot_config = bot_config

    @ui.button(label='Add Account', style=discord.ButtonStyle.success, emoji='<:user:1426148393092386896>')
    async def add_account(self, interaction: discord.Interaction, button: ui.Button):
        _ = button
        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        if not user_config:
            await interaction.response.send_message("<:exclamation:1426164277638594680> You are not registered to use this bot. Please contact the owner.", ephemeral=True)
            return
        limit = user_config.get('limit', -1)
        if limit != -1 and len(user_config.get('accounts', [])) >= limit:
            await interaction.response.send_message("<:exclamation:1426164277638594680> Sorry, you can't add more accounts limit reached.", ephemeral=True)
            return
        await interaction.response.send_modal(AddAccountModal())

    @ui.button(label='Edit Account', style=discord.ButtonStyle.primary, emoji='<:edit:1426170391218487349>')
    async def edit_account(self, interaction: discord.Interaction, button: ui.Button):
        _ = button
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        accounts = user_config.get('accounts', []) if user_config else []

        if not accounts:
            await interaction.followup.send("<:exclamation:1426164277638594680> No Account Configured Using Command `/account` & Use Button `Add Account` to added the account", ephemeral=True)
            return

        await AccountSelectionView.create(interaction, 'edit')

    @ui.button(label='Delete Account', style=discord.ButtonStyle.danger, emoji='<:trash:1426173796796203029>')
    async def delete_account(self, interaction: discord.Interaction, button: ui.Button):
        _ = button
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        db = get_db()
        user_config = await run_db(db.get_user, interaction.user.id)
        accounts = user_config.get('accounts', []) if user_config else []

        if not accounts:
            await interaction.followup.send("<:exclamation:1426164277638594680> No Account Configured Using Command `/account` & Use Button `Add Account` to added the account", ephemeral=True)
            return

        await AccountSelectionView.create(interaction, 'delete')

class AccountCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_db()

    @app_commands.command(name="account", description="Manage your linked Discord accounts")
    async def account(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            user = await ensure_active_subscription(interaction)
            if not user:
                return

            embed = create_main_account_embed(self.bot.config)
            await interaction.edit_original_response(embed=embed, view=AccountDashboard(self.bot.config))

        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            try:
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> An error occurred while processing your request.", embed=None, view=None)
            except (discord.NotFound, discord.InteractionResponded):
                pass

async def setup(bot):
    await bot.add_cog(AccountCog(bot))
