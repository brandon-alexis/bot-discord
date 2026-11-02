import os
import threading
from dotenv import load_dotenv
from server import run_server

import requests
import discord
from discord.ext import commands

threading.Thread(target=run_server, daemon=True).start()

load_dotenv()
TOKEN=os.getenv("TOKEN")


intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="$", intents=intents)

@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')
    
@bot.command()
async def test(ctx, arg):
    await ctx.send(f'Hello {arg}')

@bot.command()
async def ping(ctx, arg):
    result = requests.get("https://rickandmortyapi.com/api/character/" + arg)
    
    if result.status_code != 200:
        await ctx.send("Personaje no encontrado")
    else:
        image_url = result.json()["image"]
        print(image_url)
        await ctx.send(image_url)

bot.run(TOKEN)

