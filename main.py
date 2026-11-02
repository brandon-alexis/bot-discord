import os
from dotenv import load_dotenv

import discord
from discord.ext import commands

load_dotenv()
TOKEN=os.getenv("TOKEN")

print(TOKEN)

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="$", intents=intents)

@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')
    
@bot.command()
async def test(ctx, arg):
    await ctx.send(f'Hello {arg}')

bot.run(TOKEN)

