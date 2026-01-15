import discord
from discord.ext import commands
from discord.ui import Button, View
import asyncio
import sys
import os

# ==========================================
# CẤU HÌNH (HÃY DÙNG TOKEN MỚI SAU KHI RESET)
TOKEN = 'MTQ2MTI2MTM1NDg2NTI2Njg2Mw.G0MnXv.FUCeKgRfDOhB2XVDY3JYEAo1GJcd6QcLYG4P8k'
CHANNEL_ID = 1446167394249871382
  # ID bạn cung cấp
# ==========================================

SCRIPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Dang_Lam_LV1_project_02.py')
PYTHON_EXEC = sys.executable

# Bật full quyền
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


class RestartView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🔄 CHẠY LẠI NGAY", style=discord.ButtonStyle.green, custom_id="restart_btn")
    async def restart_button(self, interaction: discord.Interaction, button: Button):
        await interaction.response.send_message("🫡 Rõ! Đang khởi động lại hệ thống...", ephemeral=False)
        button.disabled = True
        button.label = "⏳ Đang khởi động..."
        await interaction.message.edit(view=self)
        asyncio.create_task(run_crawler_script(interaction.channel))


async def run_crawler_script(channel):
    embed = discord.Embed(title="🚀 Crawler Bắt Đầu", description="Đang khởi chạy tiến trình...", color=0x3498db)
    await channel.send(embed=embed)

    process = await asyncio.create_subprocess_exec(
        PYTHON_EXEC, SCRIPT_PATH,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()
    return_code = process.returncode

    if return_code == 0:
        embed = discord.Embed(title="✅ Hoàn Tất", description="Đã chạy xong.", color=0x2ecc71)
        await channel.send(embed=embed)
    else:
        err_msg = stderr.decode().strip()[-1000:] or "Lỗi không xác định."
        embed = discord.Embed(title="⚠️ CẢNH BÁO: Crawler Bị Dừng!", description="Có lỗi xảy ra.", color=0xe74c3c)
        embed.add_field(name="Chi tiết:", value=f"```\n{err_msg}\n```", inline=False)
        await channel.send(embed=embed, view=RestartView())


@bot.event
async def on_ready():
    print(f'🤖 Bot đã login: {bot.user}')
    print(f'🔍 Đang tìm kênh ID: {CHANNEL_ID} ...')

    try:
        # Dùng fetch_channel để ép buộc tìm từ Server về (chính xác hơn get_channel)
        channel = await bot.fetch_channel(CHANNEL_ID)
        print(f"✅ ĐÃ TÌM THẤY KÊNH: {channel.name} (Server: {channel.guild.name})")

        await channel.send(f"👋 Bot Giám Sát đã kết nối thành công vào kênh **{channel.name}**!")
        asyncio.create_task(run_crawler_script(channel))

    except discord.NotFound:
        print(f"❌ LỖI: Không tìm thấy kênh có ID {CHANNEL_ID}. Hãy kiểm tra lại số ID.")
        print("💡 Gợi ý: Chuột phải vào tên kênh chat -> Copy Channel ID (Đừng copy Category ID).")
    except discord.Forbidden:
        print(f"❌ LỖI: Bot tìm thấy kênh nhưng KHÔNG CÓ QUYỀN GỬI TIN NHẮN vào đó.")
        print("💡 Gợi ý: Vào cài đặt kênh -> Permission -> Cấp quyền 'View Channel' và 'Send Messages' cho Bot.")
    except Exception as e:
        print(f"❌ LỖI KHÁC: {e}")


if __name__ == "__main__":
    try:
        bot.run(TOKEN)
    except Exception as e:
        print(f"Không thể chạy Bot: {e}")