import copy
import discord
from discord import app_commands, ui
from discord.ext import commands
import asyncio
import aiohttp
import base64
import random
import string
import math
from datetime import datetime, timedelta, timezone
import pytz

from database import get_db, run_db
from commands.acc import validate_token
from handler.autopost import get_autopost_manager
from handler.autoreply import get_autoreply_manager
import logging

logger = logging.getLogger(__name__)
logger.handlers.clear()
logger.addHandler(logging.NullHandler())
logger.propagate = False


def _ensure_iso_str(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _sanitize_channel(channel):
    if not isinstance(channel, dict):
        return {}
    cleaned = copy.deepcopy(channel)
    cleaned.pop('attachments', None)
    cleaned.pop('_validation_status', None)
    return cleaned


def _sanitize_account(account):
    if not isinstance(account, dict):
        return {}
    cleaned = copy.deepcopy(account)
    cleaned.pop('_validation_status', None)
    cleaned['created_at'] = _ensure_iso_str(cleaned.get('created_at'))
    cleaned['added_at'] = _ensure_iso_str(cleaned.get('added_at') or datetime.now(timezone.utc))
    cleaned['channels'] = [_sanitize_channel(ch) for ch in cleaned.get('channels', [])]
    return cleaned


def _prepare_user_config_for_storage(user_config):
    if not isinstance(user_config, dict):
        return {}
    sanitized = copy.deepcopy(user_config)
    sanitized['expiry_date'] = _ensure_iso_str(sanitized.get('expiry_date'))
    sanitized['updated_at'] = _ensure_iso_str(sanitized.get('updated_at'))
    sanitized['accounts'] = [_sanitize_account(acc) for acc in sanitized.get('accounts', [])]
    return sanitized
def is_owner(interaction: discord.Interaction) -> bool:
    return str(interaction.user.id) == str(interaction.client.config.get('owner_id'))

def format_datetime_id(dt: datetime):
    if not isinstance(dt, datetime):
        return "N/A"
    return f"<t:{int(dt.timestamp())}:F>"

async def prune_accounts(user_id: str, user_config: dict, new_limit: int):
    manager_post = get_autopost_manager()
    manager_reply = get_autoreply_manager()
    
    accounts = user_config.get('accounts', [])
    if new_limit == -1 or len(accounts) <= new_limit:
        return user_config

    num_to_delete = len(accounts) - new_limit
    
    detailed_accounts = []
    for i, acc in enumerate(accounts):
        is_valid = (await validate_token(acc.get('token'))) is not None
        channels = acc.get('channels', [])
        num_channels = len(channels)
        num_inactive = len([ch for ch in channels if not ch.get('is_active')])
        detailed_accounts.append({
            'account': acc,
            'index': i,
            'num_channels': num_channels,
            'is_valid': is_valid,
            'num_inactive': num_inactive
        })

    detailed_accounts.sort(key=lambda x: (x['num_channels'], not x['is_valid'], -x['num_inactive'], -x['index']))
    
    accounts_to_delete = [item['account'] for item in detailed_accounts[:num_to_delete]]
    
    for acc in accounts_to_delete:
        if manager_reply:
            await manager_reply.stop_task(user_id, acc['token'])
        for channel in acc.get('channels', []):
            if channel.get('is_active'):
                task_key = f"{acc['token']}_{channel['channel_id']}"
                if manager_post:
                    await manager_post.remove_task(task_key)

    deleted_tokens = {acc['token'] for acc in accounts_to_delete}
    user_config['accounts'] = [acc for acc in accounts if acc['token'] not in deleted_tokens]
    
    return user_config


async def stop_all_user_tasks(user_id: str, user_config: dict | None = None):
    db = get_db()
    if user_config is None:
        user_config = await run_db(db.get_user, user_id)
    if not user_config:
        return
    manager_post = get_autopost_manager()
    manager_reply = get_autoreply_manager()
    for acc in user_config.get('accounts', []) or []:
        token = acc.get('token')
        if manager_reply and token:
            try:
                await manager_reply.stop_task(user_id, token)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
        if manager_post and token:
            for channel in acc.get('channels', []) or []:
                if channel.get('channel_id'):
                    task_key = f"{token}_{channel['channel_id']}"
                    try:
                        await manager_post.remove_task(task_key)
                    except Exception:
                        logger.exception('Unhandled exception in %s', __name__)

class ShopCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_db()
        self.packages = {
            "Perma Limited": {"price": 25000, "limit": 1, "trial": False, "desc": "Limited Accounts & Never Expires"},
            "Trial Unlimited": {"price": 30000, "limit": -1, "trial": True, "desc": "Unlimited Accounts & Expires in 30 Days"},
            "Trial Limited": {"price": 10000, "limit": 1, "trial": True, "desc": "Limited Accounts & Expires in 30 Days"}
        }
        self.package_hierarchy = {
            "Trial Limited": 0,
            "Trial Unlimited": 1,
            "Perma Limited": 2,
        }
        self.jakarta_tz = pytz.timezone('Asia/Jakarta')

    ALL_ROLE_ID = 1301011003554926672
    PERMA_ROLE_ID = 1327543532206035076
    TRIAL_ROLE_ID = 1327543942190862336

    async def apply_roles(self, interaction: discord.Interaction, package_type: str):
        try:
            target_guild = interaction.guild
            member = None
            if target_guild is not None:
                try:
                    member = target_guild.get_member(interaction.user.id) or await target_guild.fetch_member(interaction.user.id)
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
                    member = None
            else:
                for g in self.bot.guilds:
                    all_role = g.get_role(self.ALL_ROLE_ID)
                    perma_role = g.get_role(self.PERMA_ROLE_ID)
                    trial_role = g.get_role(self.TRIAL_ROLE_ID)
                    if not (all_role or perma_role or trial_role):
                        continue
                    try:
                        m = g.get_member(interaction.user.id) or await g.fetch_member(interaction.user.id)
                    except Exception:
                        logger.exception('Unhandled exception in %s', __name__)
                        m = None
                    if m:
                        target_guild = g
                        member = m
                        break

            if target_guild is None or member is None:
                return

            all_role = target_guild.get_role(self.ALL_ROLE_ID)
            perma_role = target_guild.get_role(self.PERMA_ROLE_ID)
            trial_role = target_guild.get_role(self.TRIAL_ROLE_ID)

            to_add, to_remove = [], []
            if all_role and all_role not in member.roles:
                to_add.append(all_role)
            if 'Trial' in package_type:
                if trial_role and trial_role not in member.roles:
                    to_add.append(trial_role)
                if perma_role and perma_role in member.roles:
                    to_remove.append(perma_role)
            else:
                if perma_role and perma_role not in member.roles:
                    to_add.append(perma_role)
                if trial_role and trial_role in member.roles:
                    to_remove.append(trial_role)

            if to_add:
                dedup = []
                seen = set()
                for r in to_add:
                    if r and r.id not in seen:
                        dedup.append(r)
                        seen.add(r.id)
                to_add = dedup
            if to_remove:
                dedup_r = []
                seen_r = set()
                for r in to_remove:
                    if r and r.id not in seen_r:
                        dedup_r.append(r)
                        seen_r.add(r.id)
                to_remove = dedup_r
            if to_remove:
                try:
                    await member.remove_roles(*to_remove, reason="<:chest:1426148420154036266> Subscription package change")
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
            if to_add:
                try:
                    await member.add_roles(*to_add, reason="<:chest:1426148420154036266> Subscription package assignment")
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

    shop_group = app_commands.Group(name="shop", description="Subscription shop related commands.")

    async def get_discounted_price(self, package_name, base_price):
        discount_info = await run_db(self.db.get_discount)
        if not discount_info:
            return base_price

        expiry_dt = discount_info.get('expiry')
        if expiry_dt and not expiry_dt.tzinfo:
             expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        if expiry_dt and now > expiry_dt:
            return base_price

        target_packages = discount_info.get('targets', [])
        
        normalized_package_name = package_name.lower().replace(" ", "")
        normalized_targets = [p.lower().replace(" ", "") for p in target_packages]

        if not normalized_targets or normalized_package_name in normalized_targets:
            percentage = discount_info.get('percentage', 0)
            discounted = base_price * (1 - percentage / 100)
            return int(discounted)
        
        return base_price

    @shop_group.command(name="buy", description="Buy a subscription package or limits.")
    async def buy(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        self.bot.check_user_expiration(interaction.user.id)

        user_config = await run_db(self.db.get_user, interaction.user.id)

        embed = discord.Embed(
            title="<:megaphone:1426148444786917477> Fuhuu Subscription Shop",
            description="Here are the available service packages:",
            color=self.bot.config.get('global_color')
        )
        embed.add_field(
            name="Permanent Limited <:high:1426170409782349844>",
            value="- No subscription time limit\n- Has an account addition limit based on the amount purchased",
            inline=False
        )
        embed.add_field(
            name="Trial Unlimited <:mid:1426170405957271592>",
            value="- 30-day subscription time limit\n- No account addition limit",
            inline=False
        )
        embed.add_field(
            name="Trial Limited <:low:1426170400689360926>",
            value="- 30-day subscription time limit\n- Has an account addition limit based on the amount purchased",
            inline=False
        )
        embed.add_field(
            name="*Note <:exclamation:1426164277638594680>",
            value="- Access cannot be transferred to another account after purchase.\n"
                  "- No refunds are available after purchase.\n"
                  "- Upgrading a package will not change the price.\n"
                  "- Purchasing additional limits for a 'Trial Limited' package within the first 5 days will reset the subscription duration to 30 days. After 5 days, it will only add limits without resetting the duration.\n"
                  "**Please be a smart buyer and consider your purchase carefully.**",
            inline=False
        )
        time_str = datetime.now(self.jakarta_tz).strftime('%I:%M %p')
        embed.set_footer(text=f"{self.bot.config.get('global_footer')} • {time_str}", icon_url=self.bot.config.get('global_footer_icon'))
        img_url = self.bot.config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)
            
        options = []
        emoji_map = {
            "Perma Limited": "<:high:1426170409782349844>",
            "Trial Unlimited": "<:mid:1426170405957271592>",
            "Trial Limited": "<:low:1426170400689360926>",
        }
        for name, data in self.packages.items():
            price = await self.get_discounted_price(name, data['price'])
            price_str = f"{price:,}"
            value_str = f"{price_str} IDR"
            if "Limited" in name:
                value_str += " / Limit"
            value_str += f" | {data['desc']}"
            options.append(
                discord.SelectOption(
                    label=name,
                    value=name,
                    description=value_str,
                    emoji=emoji_map.get(name, "<:chest:1426148420154036266>"),
                )
            )

        view = PackageSelectView(self, user_config=user_config, options=options)
        await interaction.edit_original_response(embed=embed, view=view)

    @shop_group.command(name="discount", description="Set or delete a global discount for packages.")
    @app_commands.check(is_owner)
    @app_commands.describe(
        action="Choose whether to set or delete the discount.",
        persentase="[SET] Discount percentage (e.g., 20 for 20%).",
        expiry="[SET] Number of days the discount is valid.",
        paket_type="[SET] Package types to discount, comma-separated (optional, leave empty for all)."
    )
    @app_commands.choices(action=[
        app_commands.Choice(name="Set a new discount", value="set"),
        app_commands.Choice(name="Delete the current discount", value="delete"),
    ])
    async def discount(self, interaction: discord.Interaction, action: str, persentase: int = None, expiry: int = None, paket_type: str = None):
        await interaction.response.defer(ephemeral=True)
        if not self.bot.check_user_expiration(interaction.user.id):
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Your trial subscription has expired. Please contact the owner or use `/shop buy` to purchase a new subscription.")
            return
        if action == "set":
            if persentase is None or expiry is None:
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> When setting a discount, 'persentase' and 'expiry' are required.")
                return

            if not (0 < persentase <= 100):
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Percentage must be between 1 and 100.")
                return

            expiry_date = datetime.now(timezone.utc) + timedelta(days=expiry)
            targets = [p.strip() for p in paket_type.split(',')] if paket_type else []

            discount_data = {
                "percentage": persentase,
                "expiry": expiry_date,
                "targets": targets
            }
            await run_db(self.db.update_discount, discount_data)
            
            target_str = ", ".join(targets) if targets else "All packages"
            await interaction.edit_original_response(content=f"<:cashback:1426148413640019979> A {persentase}% discount has been set for **{target_str}** and will end on {format_datetime_id(expiry_date)}.")
        
        elif action == "delete":
            discount_data = await run_db(self.db.get_discount)
            if not discount_data:
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> There is no active discount to delete.")
                return

            result = await run_db(self.db.remove_discount)
            if isinstance(result, (int, float)):
                deleted = int(result)
            else:
                deleted = getattr(result, 'deleted_count', 0)
            if deleted:
                await interaction.edit_original_response(content="<:clover:1426170416606478437> The active discount has been successfully deleted.")
            else:
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Failed to delete the discount. Please check the logs.")

    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure):
            await interaction.response.send_message("<:exclamation:1426164277638594680> Only the bot owner can use this command.", ephemeral=True, delete_after=10)
        else:
            if not interaction.response.is_done():
                await interaction.response.send_message("<:exclamation:1426164277638594680> An error occurred.", ephemeral=True)

class ConfirmNoResetView(ui.View):
    def __init__(self, original_interaction: discord.Interaction, shop_cog: ShopCog, package_name: str, limit: int):
        super().__init__(timeout=300)
        self.original_interaction = original_interaction
        self.shop_cog = shop_cog
        self.package_name = package_name
        self.limit = limit

    @ui.button(label="Confirm", style=discord.ButtonStyle.secondary, emoji="<:cashier:1426164280788779018>")
    async def confirm_button(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True, thinking=True)
        await create_transaction(interaction, self.shop_cog, self.package_name, self.limit, is_edit=True, keep_expiry=True)

class LimitModal(ui.Modal, title="Add Account Limit"):
    limit_input = ui.TextInput(label="Limit", placeholder="Enter the number of account limits you want to buy", required=True)

    def __init__(self, shop_cog: ShopCog, package_name: str, original_interaction: discord.Interaction):
        super().__init__()
        self.shop_cog = shop_cog
        self.package_name = package_name
        self.original_interaction = original_interaction

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            limit = int(self.limit_input.value)
            if limit <= 0:
                raise ValueError
        except ValueError:
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Invalid limit. Please enter a positive number.")
            return

        await create_transaction(interaction, self.shop_cog, self.package_name, limit, is_edit=True)


class ReactivateModal(ui.Modal, title="Renew Subscription"):
    confirmation_input = ui.TextInput(
        label="Confirmation",
        placeholder="Type confirm to renew your package and type delete if u need change package",
        required=True
    )

    def __init__(self, shop_cog: ShopCog, user_config: dict, original_interaction: discord.Interaction):
        super().__init__(timeout=300)
        self.shop_cog = shop_cog
        self.user_config = user_config
        self.original_interaction = original_interaction
        self.add_item(self.confirmation_input)

    async def on_submit(self, interaction: discord.Interaction):
        choice = (self.confirmation_input.value or "").strip().lower()
        user_id = str(interaction.user.id)
        if choice == "confirm":
            package_name = self.user_config.get('package_type')
            if not package_name or 'trial' not in package_name.lower():
                await interaction.response.defer(ephemeral=True)
                await interaction.followup.send("<:exclamation:1426164277638594680> Only trial packages support reactivation.", ephemeral=True)
                return
            limit = self.user_config.get('limit', 1) if "Limited" in package_name else 1
            await interaction.response.defer(ephemeral=True, thinking=True)
            await create_transaction(
                interaction,
                self.shop_cog,
                package_name,
                limit=limit if limit > 0 else 1,
                is_edit=True,
                is_renewal=True
            )
        elif choice == "delete":
            await interaction.response.defer(ephemeral=True, thinking=True)
            await stop_all_user_tasks(user_id, self.user_config)
            await run_db(self.shop_cog.db.delete_user, user_id)
            await interaction.edit_original_response(
                content="<:trash:1426173796796203029> Your subscription data has been deleted. Feel free to purchase a new package anytime.",
                embed=None,
                view=None
            )
        else:
            await interaction.response.defer(ephemeral=True)
            await interaction.followup.send("<:exclamation:1426164277638594680> Please type `confirm` or `delete`.", ephemeral=True)

class PackageSelectView(ui.View):
    def __init__(self, shop_cog: ShopCog, user_config: dict | None = None, options: list | None = None):
        super().__init__(timeout=300)
        self.shop_cog = shop_cog
        self.user_config = user_config

        option_items = options or []
        self.select_menu = ui.Select(placeholder="Select a subscription package...", options=option_items)
        self.select_menu.callback = self.select_callback
        self.add_item(self.select_menu)

        if self.user_config and self.user_config.get('package_type', '').startswith("Trial") and self.user_config.get('expiry_date'):
            expiry_date = self.user_config['expiry_date']
            if expiry_date.tzinfo is None:
                expiry_date = expiry_date.replace(tzinfo=timezone.utc)
            
            time_left = expiry_date - datetime.now(timezone.utc)
            if time_left <= timedelta(days=1):
                renew_button = ui.Button(label="Renew Subscription", style=discord.ButtonStyle.success, emoji="<:delete:1426170386734911578>")
                renew_button.callback = self.renew_callback
                self.add_item(renew_button)

        if (
            self.user_config
            and self.user_config.get('package_type', '').lower().startswith('trial')
            and not self.user_config.get('is_active', True)
        ):
            reactivate_btn = ui.Button(label="Reactivate", style=discord.ButtonStyle.secondary, emoji="<:cashier:1426164280788779018>")
            reactivate_btn.callback = self.reactivate_callback
            self.add_item(reactivate_btn)
    
    async def renew_callback(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        package_name = self.user_config['package_type']
        limit = self.user_config.get('limit', 1) if "Limited" in package_name else 1
        await create_transaction(interaction, self.shop_cog, package_name, limit, is_edit=True, is_renewal=True)

    async def select_callback(self, interaction: discord.Interaction):
        package_name = interaction.data['values'][0]
        if "Limited" in package_name:
            await interaction.response.send_modal(LimitModal(self.shop_cog, package_name, original_interaction=interaction))
        else:
            await interaction.response.defer(ephemeral=True, thinking=True)
            await create_transaction(interaction, self.shop_cog, package_name, is_edit=True)

    async def reactivate_callback(self, interaction: discord.Interaction):
        if interaction.response.is_done():
            return
        await interaction.response.send_modal(
            ReactivateModal(self.shop_cog, self.user_config, interaction)
        )

class PaymentView(ui.View):
    def __init__(self, shop_cog: ShopCog, transaction_data: dict, package_name: str, limit: int, total_amount: int, is_renewal: bool, keep_expiry: bool, original_interaction: discord.Interaction):
        super().__init__(timeout=900)
        self.shop_cog = shop_cog
        self.transaction_id = transaction_data['transaction_id']
        self.order_id = transaction_data['order_id']
        self.package_name = package_name
        self.limit = limit
        self.total_amount = total_amount
        self.check_count = 0
        self.is_renewal = is_renewal
        self.keep_expiry = keep_expiry
        self.original_interaction = original_interaction

    @ui.button(label="Check Payment", style=discord.ButtonStyle.secondary, emoji="<:delete:1426170386734911578>")
    async def check_button(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer(ephemeral=True)
        server_key = self.shop_cog.bot.config['midtrans']['server_key']
        auth = base64.b64encode(f"{server_key}:".encode()).decode()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {auth}"
        }
        url = f"https://api.midtrans.com/v2/{self.order_id}/status"

        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as response:
                    if response.status >= 400:
                        text = await response.text()
                        raise aiohttp.ClientResponseError(request_info=response.request_info, history=response.history, status=response.status, message=text)
                    data = await response.json(content_type=None)
            status = data.get('transaction_status')

            if status == 'settlement':
                await interaction.edit_original_response(content="<:clover:1426170416606478437> Payment confirmed! Processing your purchase...")
                await process_successful_purchase(interaction, self.shop_cog, self)
            elif status == 'expire':
                await interaction.edit_original_response(content="<:exclamation:1426164277638594680> The QR code has expired. Please try the purchase again.", view=None)
                try:
                    original_interaction = self.children[0].custom_id.split(':')[1]
                    original_interaction.edit_original_response(content="<:exclamation:1426164277638594680> This transaction has expired.", view=None, embed=None)
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
            elif status == 'pending':
                self.check_count += 1
                await interaction.edit_original_response(content=f"<:exclamation:1426164277638594680> Please complete the payment first. Check attempt: {self.check_count}.")
            else:
                await interaction.edit_original_response(content=f"<:clover:1426170416606478437> Transaction status: {status}. <:exclamation:1426164277638594680> Please try again later.")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            await interaction.edit_original_response(content=f"Failed to check payment status: {e}")

async def create_transaction(interaction: discord.Interaction, shop_cog: ShopCog, package_name: str, limit: int = 1, is_edit: bool = False, is_renewal: bool = False, keep_expiry: bool = False):
    user_config = await run_db(shop_cog.db.get_user, interaction.user.id)
    if not is_renewal and user_config and 'package_type' in user_config:
        current_package_name = user_config['package_type']
        
        current_rank = shop_cog.package_hierarchy.get(current_package_name, -1)
        if current_package_name and 'perma' in current_package_name.lower() and 'unlimited' in current_package_name.lower():
            current_rank = 99
        new_rank = shop_cog.package_hierarchy.get(package_name, -1)

        if new_rank < current_rank:
            await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Sorry, you cannot downgrade your subscription.", view=None, embed=None)
            return

    package_info = shop_cog.packages[package_name]
    base_price = await shop_cog.get_discounted_price(package_name, package_info['price'])
    
    amount = base_price * limit
    tax = max(1, math.ceil(amount * 0.008)) if amount > 0 else 0
    total_amount = amount + tax
    
    server_key = shop_cog.bot.config['midtrans']['server_key']
    auth = base64.b64encode(f"{server_key}:".encode()).decode()
    random_suffix = ''.join(random.choices(string.digits, k=5))
    order_id = f"{package_name.replace(' ', '-')}-{random_suffix}"

    payload = {
        "payment_type": "qris",
        "transaction_details": {
            "order_id": order_id,
            "gross_amount": total_amount
        },
        "customer_details": {
            "first_name": interaction.user.name,
        },
        "qris": {
            "acquirer": "gopay"
        }
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": f"Basic {auth}"
    }

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post("https://api.midtrans.com/v2/charge", json=payload, headers=headers) as res:
                if res.status >= 400:
                    text = await res.text()
                    raise aiohttp.ClientResponseError(request_info=res.request_info, history=res.history, status=res.status, message=text)
                data = await res.json(content_type=None)

        if data.get("status_code") == "201":
            qr_url = next((action['url'] for action in data.get('actions', []) if action['name'] == 'generate-qr-code'), None)
            
            expiry_date_str = "Never"
            if package_info['trial']:
                expiry_date_str = f"<t:{int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())}:F>"

            embed = discord.Embed(
                title="<:scroll:1426148423157022782> Transaction Details",
                description="Here are the details for your subscription purchase.",
                color=shop_cog.bot.config.get('global_color')
            )
            transaction_details = (
                f"**<:dot:1426148484146270269> Transaction ID**: ```{data['transaction_id']}```\n"
                f"**<:dot:1426148484146270269> Subscription Type**: {package_name}\n"
                f"**<:dot:1426148484146270269> Limit**: {'Unlimited' if package_info['limit'] == -1 else limit}\n"
                f"**<:dot:1426148484146270269> Expiry**: {expiry_date_str}\n"
                f"**<:dot:1426148484146270269> Total**: {total_amount:,} IDR"
            )
            embed.add_field(name="Transaction Details <:cashier:1426164280788779018>", value=transaction_details, inline=False)
            embed.add_field(name="Payment QR Code", value="Scan the QR code below using any application that supports QRIS. <:coin:1426148404253429831>", inline=False)
            time_str = datetime.now(shop_cog.jakarta_tz).strftime('%I:%M %p')
            embed.set_footer(text=f"{shop_cog.bot.config.get('global_footer')} • {time_str}", icon_url=shop_cog.bot.config.get('global_footer_icon'))
            
            if qr_url:
                embed.set_image(url=qr_url)

            view = PaymentView(shop_cog, data, package_name, limit, total_amount, is_renewal, keep_expiry, original_interaction=interaction)
            
            await interaction.edit_original_response(embed=embed, view=view, content=None)
        else:
            error_message = f"<:exclamation:1426164277638594680> Failed to create transaction: {data.get('status_message')}"
            await interaction.edit_original_response(content=error_message, embed=None, view=None)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        error_message = f"<:exclamation:1426164277638594680> Error contacting the payment gateway: {e}"
        await interaction.edit_original_response(content=error_message, embed=None, view=None)

async def process_successful_purchase(interaction: discord.Interaction, shop_cog: ShopCog, payment_view: PaymentView):
    original_interaction = getattr(payment_view, 'original_interaction', interaction)
    
    user = original_interaction.user
    user_id_str = str(user.id)
    
    user_config = await run_db(shop_cog.db.get_user, user_id_str)
    is_new_user = user_config is None
    
    if is_new_user:
        user_config = {
            'user_id': user_id_str,
            'username': user.name,
            'display_name': user.display_name,
            'accounts': [],
            'is_active': True
        }

    old_package_name = user_config.get('package_type')
    package_name = payment_view.package_name
    limit_purchased = payment_view.limit
    is_renewal = payment_view.is_renewal
    keep_expiry = payment_view.keep_expiry
    
    now = datetime.now(timezone.utc)
    current_expiry = user_config.get('expiry_date')
    if current_expiry and current_expiry.tzinfo is None:
        current_expiry = current_expiry.replace(tzinfo=timezone.utc)
    remaining = timedelta(0)
    if current_expiry and current_expiry > now:
        remaining = current_expiry - now

    new_limit = user_config.get('limit')
    new_expiry = current_expiry

    if keep_expiry:
        if "Limited" in package_name:
            base_limit = user_config.get('limit', 0) or 0
            new_limit = base_limit + limit_purchased
        else:
            new_limit = -1
    else:
        if package_name == "Trial Unlimited":
            new_limit = -1
            new_expiry = now + remaining + timedelta(days=30)
        elif package_name == "Trial Limited":
            base_limit = user_config.get('limit', 0) or 0
            if is_new_user or old_package_name != "Trial Limited":
                base_limit = 0
            if is_renewal:
                new_limit = base_limit if base_limit > 0 else limit_purchased
            else:
                new_limit = base_limit + limit_purchased
            new_expiry = now + timedelta(days=30)
        else:
            new_limit = -1 if "Unlimited" in package_name else limit_purchased
            new_expiry = None

    user_config.update({
        'username': user.name,
        'display_name': user.display_name,
        'package_type': package_name,
        'limit': new_limit,
        'expiry_date': new_expiry,
        'is_active': True
    })
    
    user_config = await prune_accounts(user_id_str, user_config, new_limit)

    sanitized_user = _prepare_user_config_for_storage(user_config)
    await run_db(shop_cog.db.add_user, sanitized_user)
    try:
        await shop_cog.apply_roles(original_interaction, user_config.get('package_type', ''))
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)

    success_embed = discord.Embed(
        title="<:clover:1426170416606478437> Transaction Successful",
        description="Thank you for purchasing our product! For instructions on how to use the auto-post and auto-reply features, please visit the tutorial channel.",
        color=shop_cog.bot.config.get('global_color')
    )
    time_str = datetime.now(shop_cog.jakarta_tz).strftime('%I:%M %p')
    success_embed.set_footer(text=f"{shop_cog.bot.config.get('global_footer')} • {time_str}", icon_url=shop_cog.bot.config.get('global_footer_icon'))
    img_url = shop_cog.bot.config.get('global_img_url')
    if img_url:
        success_embed.set_image(url=img_url)
    
    try:
        await original_interaction.edit_original_response(embed=success_embed, view=None)
    except (discord.NotFound, discord.InteractionResponded):
        await original_interaction.followup.send(embed=success_embed, ephemeral=True)

    
    await send_dm_notification(user, shop_cog, user_config, is_new_user)
    await send_log_webhook(original_interaction, shop_cog, payment_view, user_config, old_package_name)


async def send_dm_notification(user: discord.User, shop_cog: ShopCog, user_config: dict, is_new_user: bool):
    try:
        config = shop_cog.bot.config
        
        if is_new_user:
            title = "<:clover:1426170416606478437> Successfully Added to Database"
            desc = "You have been added to the database and can now use the auto-post and auto-reply features."
            content = f"Hi {user.mention}, you have been added to the database. You can now access other bot commands."
        else:
            title = "<:clover:1426170416606478437> Your Subscription Has Been Updated"
            desc = "Thank you for your purchase! Here are your current subscription details."
            content = f"Hi {user.mention}, your subscription has been updated."

        embed = discord.Embed(
            title=title,
            description=desc,
            color=config.get('global_color', 0x00ff00),
            timestamp=datetime.now(timezone.utc)
        )
        if user.display_avatar:
            embed.set_thumbnail(url=user.display_avatar.url)
        
        user_details_value = (
            f"**<:dot:1426148484146270269> Display Name**: {user.display_name}\n"
            f"**<:dot:1426148484146270269> Username**: {user.name}\n"
            f"**<:dot:1426148484146270269> User ID**: `{user.id}`\n"
            f"**<:dot:1426148484146270269> Account Created**: {format_datetime_id(user.created_at)}"
        )
        embed.add_field(name="Your Account Details <:scroll:1426148423157022782>", value=user_details_value, inline=False)
        
        limit_str = "Unlimited" if user_config.get('limit', 0) == -1 else str(user_config.get('limit', 'N/A'))
        expiry_str = "Never" if user_config.get('expiry_date') is None else format_datetime_id(user_config['expiry_date'])

        sub_details = (
            f"**<:dot:1426148484146270269> Package**: {user_config.get('package_type', 'N/A')}\n"
            f"**<:dot:1426148484146270269> Account Limit**: {limit_str}\n"
            f"**<:dot:1426148484146270269> Expires**: {expiry_str}"
        )
        embed.add_field(name="Subscription Details <:chest:1426148420154036266>", value=sub_details, inline=False)
        
        if config.get('global_footer'):
            time_str = datetime.now(shop_cog.jakarta_tz).strftime('%I:%M %p')
            embed.set_footer(text=f"{config.get('global_footer')} • {time_str}", icon_url=config.get('global_footer_icon'))
        img_url = config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)

        await user.send(content=content, embed=embed)
    except discord.Forbidden:
        pass
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)

async def send_log_webhook(interaction: discord.Interaction, shop_cog: ShopCog, payment_view: PaymentView, user_config: dict, old_package_name: str = None):
    log_url = shop_cog.bot.config.get('shop', {}).get('log_webhook')
    if not log_url:
        return

    title = "<:cashier:1426164280788779018> New Purchase"
    description = f"{interaction.user.mention} has purchased a new subscription."
    if old_package_name and old_package_name != payment_view.package_name:
        title = "Subscription Updated <:cashier:1426164280788779018>"
        description = f"{interaction.user.mention} has updated their subscription."
    elif old_package_name and old_package_name == payment_view.package_name:
        title = "Limit Added <:cashier:1426164280788779018>"
        description = f"{interaction.user.mention} has added more limits to their subscription."

    expiry_str = "Never"
    if user_config.get('expiry_date'):
        expiry_str = format_datetime_id(user_config['expiry_date'])
    
    limit_str = 'Unlimited'
    if "Limited" in payment_view.package_name:
        limit_str = str(user_config['limit'])

    embed = discord.Embed(
        title=title,
        description=description,
        color=shop_cog.bot.config.get('global_color')
    )

    if old_package_name and old_package_name != payment_view.package_name:
        embed.add_field(
            name="Package Change <:chest:1426148420154036266>",
            value=f"**From**: {old_package_name}\n**To**: {payment_view.package_name}",
            inline=False
        )
    
    details_value = (
        f"**<:dot:1426148484146270269> Package Type**: {payment_view.package_name}\n"
        f"**<:dot:1426148484146270269> Limit**: {limit_str}\n"
        f"**<:dot:1426148484146270269> Expiry Time**: {expiry_str}\n"
        f"**<:dot:1426148484146270269> Total**: {payment_view.total_amount:,} IDR"
    )
    embed.add_field(name="Product Details <:scroll:1426148423157022782>", value=details_value, inline=False)
    time_str = datetime.now(shop_cog.jakarta_tz).strftime('%I:%M %p')
    embed.set_footer(text=f"{shop_cog.bot.config.get('global_footer')} • {time_str}", icon_url=shop_cog.bot.config.get('global_footer_icon'))
    img_url = shop_cog.bot.config.get('global_img_url')
    if img_url:
        embed.set_image(url=img_url)

    payload = {
        "embeds": [embed.to_dict()]
    }
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(log_url, json=payload):
                pass
    except (aiohttp.ClientError, asyncio.TimeoutError):
        pass

async def setup(bot: commands.Bot):
    await bot.add_cog(ShopCog(bot))
