import discord
from discord.ext import commands

class AutoReply(commands.Cog):
    def __init__(self, bot, message: str):
        self.bot = bot
        self.reply_message = message

    # Ketika ada DM masuk
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        # Autoreply di DM
        if isinstance(message.channel, discord.DMChannel):
            try:
                await message.channel.send(self.reply_message)
            except:
                pass
        
        # Autoreply kalau disebut (mention)
        if self.bot.user in message.mentions:
            try:
                await message.reply(self.reply_message)
            except:
                pass


# Fungsi untuk load di main.py
def setup(bot, autoreply_message: str):
    bot.add_cog(AutoReply(bot, autoreply_message))
