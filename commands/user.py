import discord
from discord import app_commands
from discord.ext import commands
from discord.utils import format_dt
from datetime import datetime, timedelta, timezone

from database import get_db, run_db
import logging

logger = logging.getLogger(__name__)
logger.handlers.clear()
logger.addHandler(logging.NullHandler())
logger.propagate = False
def is_owner(interaction: discord.Interaction) -> bool:
    return str(interaction.user.id) == str(interaction.client.config.get('owner_id'))

def format_datetime(dt: datetime):
    if not isinstance(dt, datetime):
        return "N/A"
    return f"<t:{int(dt.timestamp())}:F>"


 

class UserCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = get_db()

    ALL_ROLE_ID = 1301011003554926672
    PERMA_ROLE_ID = 1327543532206035076
    TRIAL_ROLE_ID = 1327543942190862336

    async def _apply_roles(self, interaction: discord.Interaction, member: discord.Member, package_type: str):
        if not interaction.guild or not isinstance(member, discord.Member):
            return
        try:
            roles_to_add = []
            roles_to_remove = []
            all_role = interaction.guild.get_role(self.ALL_ROLE_ID)
            perma_role = interaction.guild.get_role(self.PERMA_ROLE_ID)
            trial_role = interaction.guild.get_role(self.TRIAL_ROLE_ID)
            if all_role and all_role not in member.roles:
                roles_to_add.append(all_role)
            if 'Trial' in package_type:
                if trial_role and trial_role not in member.roles:
                    roles_to_add.append(trial_role)
                if perma_role and perma_role in member.roles:
                    roles_to_remove.append(perma_role)
            else:
                if perma_role and perma_role not in member.roles:
                    roles_to_add.append(perma_role)
                if trial_role and trial_role in member.roles:
                    roles_to_remove.append(trial_role)
            if roles_to_remove:
                try:
                    await member.remove_roles(*roles_to_remove, reason="Subscription package change <:chest:1426148420154036266>")
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
            if roles_to_add:
                try:
                    await member.add_roles(*roles_to_add, reason="Subscription package assignment <:chest:1426148420154036266>")
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

    def _format_limit(self, limit_value):
        if limit_value is None:
            return "N/A"
        try:
            if int(limit_value) == -1:
                return "Unlimited"
        except (ValueError, TypeError):
            return str(limit_value)
        return str(limit_value)

    def _format_expiry(self, user_data):
        package_type = (user_data.get('package_type') or '').lower()
        expiry_date = user_data.get('expiry_date')
        if 'perma' in package_type:
            return 'Never'
        if isinstance(expiry_date, datetime):
            if expiry_date.tzinfo is None:
                expiry_date = expiry_date.replace(tzinfo=timezone.utc)
            return format_datetime(expiry_date)
        if expiry_date:
            return str(expiry_date)
        return 'N/A'

    def _build_user_list_embed(self, users, current_page, total_pages, actual_users: dict | None = None):
        embed = discord.Embed(
            title="<:scroll:1426148423157022782> User List",
            description="Here are the users registered in the database.",
            color=self.bot.config.get('global_color', 0)
        )
        if total_pages > 1:
            embed.set_author(name=f"Page {current_page}/{total_pages}")

        actual_users = actual_users or {}

        for user in users:
            actual = actual_users.get(str(user.get('user_id')))
            if actual:
                fallback_display = actual.global_name or actual.display_name or actual.name
                fallback_username = actual.name
            else:
                fallback_display = None
                fallback_username = None

            display_name = user.get('display_name') or fallback_display or user.get('username') or 'Unknown'
            username = user.get('username') or fallback_username or 'Unknown'
            user_id = user.get('user_id') or 'Unknown'
            paket_type = user.get('package_type') or 'Unknown'
            limit_value = self._format_limit(user.get('limit'))
            expiry_value = self._format_expiry(user)

            field_value = (
                f"**<:dot:1426148484146270269> Username** : {username}\n"
                f"**<:dot:1426148484146270269> User ID** : {user_id}\n"
                f"**<:dot:1426148484146270269> Package Type** : {paket_type}\n"
                f"**<:dot:1426148484146270269> Limit** : {limit_value}\n"
                f"**<:dot:1426148484146270269> Expiry** : {expiry_value}"
            )
            embed.add_field(name=f"<:user:1426148393092386896> {display_name}", value=field_value, inline=False)

        tz = getattr(self.bot, 'jakarta_tz', timezone.utc)
        now = datetime.now(tz)
        time_only = now.strftime('%I:%M %p').lstrip('0')
        today = now.date()
        yesterday = (now - timedelta(days=1)).date()
        if now.date() == today:
            human_time = f"Today {time_only}"
        elif now.date() == yesterday:
            human_time = f"Yesterday {time_only}"
        else:
            human_time = now.strftime('%d-%m-%Y %I:%M %p').replace(' 0', ' ').lstrip('0')
        footer_base = self.bot.config.get('global_footer')
        footer_text = f"{footer_base} • {human_time}" if footer_base else human_time
        embed.set_footer(
            text=footer_text,
            icon_url=self.bot.config.get('global_footer_icon')
        )
        img_url = self.bot.config.get('global_img_url')
        if img_url:
            embed.set_image(url=img_url)
        return embed

    user_group = app_commands.Group(name="user", description="User management commands for the bot owner.")

    @user_group.command(name="add", description="Add or update a user's subscription in the database.")
    @app_commands.check(is_owner)
    @app_commands.describe(
        user="The user to add or update.",
        package_type="The subscription package type for the user.",
        limit="Limit for 'Limited' packages (ignored for 'Unlimited').",
        expiry_day="[Trial Only] Number of days until expiry from now. Defaults to 30."
    )
    @app_commands.choices(package_type=[
        app_commands.Choice(name="Permanent Unlimited", value="Perma Unlimited"),
        app_commands.Choice(name="Permanent Limited", value="Perma Limited"),
        app_commands.Choice(name="Trial Unlimited", value="Trial Unlimited"),
        app_commands.Choice(name="Trial Limited", value="Trial Limited"),
    ])
    async def add_user(self, interaction: discord.Interaction, user: discord.User, package_type: str, limit: int = None, expiry_day: int = None, isactive: str | None = None):
        await interaction.response.defer(ephemeral=True)

        now = datetime.now(timezone.utc)
        is_trial_new = "Trial" in package_type
        is_limited_new = "Limited" in package_type

        existing = await run_db(self.db.get_user, str(user.id))
        accounts = existing.get('accounts', []) if existing else []

        if is_trial_new:
            if existing:
                if existing.get('package_type') == package_type:
                    if expiry_day is not None:
                        expiry_date = now + timedelta(days=expiry_day)
                    else:
                        expiry_date = existing.get('expiry_date')
                else:
                    days = expiry_day if expiry_day is not None else 30
                    expiry_date = now + timedelta(days=days)
            else:
                days = expiry_day if expiry_day is not None else 30
                expiry_date = now + timedelta(days=days)
        else:
            expiry_date = None

        if is_limited_new:
            if limit is not None:
                new_limit = int(limit)
            elif existing and existing.get('package_type') == package_type and existing.get('limit') is not None:
                new_limit = int(existing.get('limit'))
            else:
                new_limit = 1
        else:
            new_limit = -1

        if new_limit != -1 and len(accounts) > new_limit:
            remove_count = len(accounts) - new_limit
            ranked = []
            for acc in accounts:
                chans = acc.get('channels', []) or []
                off = sum(1 for ch in chans if not ch.get('is_active'))
                total = len(chans)
                added = acc.get('added_at')
                added_ts = 0
                try:
                    if isinstance(added, datetime):
                        added_ts = added.timestamp()
                    elif isinstance(added, str):
                        added_ts = datetime.fromisoformat(added).timestamp()
                except Exception:
                    logger.exception('Unhandled exception in %s', __name__)
                    added_ts = 0
                ranked.append((-off, total, -added_ts, acc))
            ranked.sort()
            to_remove_set = set()
            for item in ranked[:remove_count]:
                to_remove_set.add(id(item[3]))
            accounts = [acc for acc in accounts if id(acc) not in to_remove_set]

        user_config = existing or {}
        user_config['user_id'] = str(user.id)
        user_config['username'] = user.name
        user_config['display_name'] = user.global_name or user.display_name or user.name
        user_config['package_type'] = package_type
        user_config['limit'] = new_limit
        user_config['expiry_date'] = expiry_date
        user_config['accounts'] = accounts
        if isactive is None:
            user_config['is_active'] = True
        else:
            user_config['is_active'] = str(isactive).strip().lower() in ("true", "1", "yes", "on")

        await run_db(self.db.add_user, user_config)

        try:
            if interaction.guild:
                member = interaction.guild.get_member(user.id) or await interaction.guild.fetch_member(user.id)
                await self._apply_roles(interaction, member, package_type)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

        await interaction.edit_original_response(content=f"<:clover:1426170416606478437> Successfully added or updated {user.mention} in the database.")

        try:
            embed = discord.Embed(
                title="<:clover:1426170416606478437> Successfully Added to Database",
                description="You have been added to the database and can now use the auto-post and auto-reply features.",
                color=self.bot.config.get('global_color', 0x00ff00),
                timestamp=datetime.now(timezone.utc)
            )
            if user.display_avatar:
                embed.set_thumbnail(url=user.display_avatar.url)
            
            value = (
                f"**<:dot:1426148484146270269> Display Name**: {user.display_name}\n"
                f"**<:dot:1426148484146270269> Username**: {user.name}\n"
                f"**<:dot:1426148484146270269> User ID**: `{user.id}`\n"
                f"**<:dot:1426148484146270269> Account Created**: {format_datetime(user.created_at)}"
            )
            embed.add_field(name="<:user:1426148393092386896> Your Account Details", value=value, inline=False)
            
            if self.bot.config.get('global_footer'):
                embed.set_footer(text=self.bot.config['global_footer'], icon_url=self.bot.config.get('global_footer_icon'))
            img_url = self.bot.config.get('global_img_url')
            if img_url:
                embed.set_image(url=img_url)

            await user.send(
                content=f"Hi <:user:1426148393092386896> {user.mention}, you have been added to the database. You can now access other bot commands.",
                embed=embed
            )
        except discord.Forbidden:
            pass
        except Exception as e:
            logger.exception('Unhandled exception in %s', __name__)
            print(f"Failed to send DM to user {user.id}: {e}")

    @user_group.command(name="info", description="Show detailed information for a registered user.")
    @app_commands.describe(user="The user to display information for")
    async def info_user(self, interaction: discord.Interaction, user: discord.User):
        await interaction.response.defer(ephemeral=True)
        user_config = await run_db(self.db.get_user, str(user.id))
        if not user_config:
            await interaction.edit_original_response(content=f"<:exclamation:1426164277638594680> User {user.mention} is not registered in the autopost database.")
            return

        embed = self._build_user_list_embed([user_config], 1, 1, actual_users={str(user.id): user})
        embed.title = "<:scroll:1426148423157022782> User Info"
        embed.description = f"Here is the complete information for {user.mention}."
        await interaction.edit_original_response(embed=embed)

    @user_group.command(name="delete", description="Remove a user from the database.")
    @app_commands.check(is_owner)
    @app_commands.describe(user="The user to remove.", reason="The reason for removal.")
    async def delete_user(self, interaction: discord.Interaction, user: discord.User, reason: str):
        await interaction.response.defer(ephemeral=True)

        user_config = await run_db(self.db.get_user, str(user.id))
        if not user_config:
            await interaction.edit_original_response(content=f"User <:user:1426148393092386896> {user.mention} was not found in the database.")
            return

        result = await run_db(self.db.delete_user, str(user.id))
        deleted = result
        if not isinstance(deleted, (int, float)):
            deleted = getattr(result, 'deleted_count', 0)
        deletion_ok = bool(deleted)
        if deletion_ok:
            await interaction.edit_original_response(content=f"User <:user:1426148393092386896> {user.mention} has been successfully deleted from the database.")
            try:
                if interaction.guild:
                    member = interaction.guild.get_member(user.id) or await interaction.guild.fetch_member(user.id)
                    roles = [interaction.guild.get_role(rid) for rid in [self.ALL_ROLE_ID, self.PERMA_ROLE_ID, self.TRIAL_ROLE_ID]]
                    roles = [r for r in roles if r]
                    if roles:
                        try:
                            await member.remove_roles(*roles, reason="User deleted from service")
                        except Exception:
                            logger.exception('Unhandled exception in %s', __name__)
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
            try:
                embed = discord.Embed(
                    title="<:user:1426148393092386896> Successfully Removed from Database",
                    description="You have been removed from the database and can no longer use the auto-post and auto-reply features.",
                    color=self.bot.config.get('global_color', 0xff0000),
                    timestamp=datetime.now(timezone.utc)
                )
                if user.display_avatar:
                    embed.set_thumbnail(url=user.display_avatar.url)

                account_details = (
                    f"**<:dot:1426148484146270269> Display Name**: {user.display_name}\n"
                    f"**<:dot:1426148484146270269> Username**: {user.name}\n"
                    f"**<:dot:1426148484146270269> User ID**: `{user.id}`\n"
                    f"**<:dot:1426148484146270269> Account Created**: {format_datetime(user.created_at)}"
                )
                embed.add_field(name="<:scroll:1426148423157022782> Your Account Details", value=account_details, inline=False)
                embed.add_field(name="<:exclamation:1426164277638594680> Reason", value=f"```{reason}```", inline=False)

                if self.bot.config.get('global_footer'):
                    embed.set_footer(text=self.bot.config['global_footer'], icon_url=self.bot.config.get('global_footer_icon'))
                img_url = self.bot.config.get('global_img_url')
                if img_url:
                    embed.set_image(url=img_url)

                await user.send(
                    content=f"Hi <:user:1426148393092386896> {user.mention}, you have been removed from the database. You can no longer access the bot's commands.",
                    embed=embed
                )
            except discord.Forbidden:
                pass
            except Exception as e:
                logger.exception('Unhandled exception in %s', __name__)
                print(f"Failed to send removal DM to user {user.id}: {e}")
        else:
            await interaction.edit_original_response(content=f"<:exclamation:1426164277638594680> An error occurred while trying to delete {user.mention}.")

    async def cog_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if interaction.response.is_done():
            try:
                if isinstance(error, app_commands.CheckFailure):
                    await interaction.edit_original_response(content="<:exclamation:1426164277638594680> Sorry, only the bot owner can use this command.")
                else:
                    print(f"An error occurred in user command: {error}")
                    await interaction.edit_original_response(content="<:exclamation:1426164277638594680> An unexpected error occurred. Please check the logs.")
            except discord.NotFound:
                pass
            return

        try:
            if isinstance(error, app_commands.CheckFailure):
                await interaction.response.send_message("<:exclamation:1426164277638594680> Sorry, only the bot owner can use this command.", ephemeral=True)
            else:
                print(f" An error occurred in user command: {error}")
                await interaction.response.send_message("<:exclamation:1426164277638594680> An unexpected error occurred. Please check the logs.", ephemeral=True)
        except discord.InteractionResponded:
            pass
        except discord.NotFound:
            pass

async def setup(bot: commands.Bot):
    await bot.add_cog(UserCog(bot))
