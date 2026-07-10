# alert_system/telegram_alert.py
import requests
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def send_telegram_alert(message):
    """إرسال تنبيه عبر التيليجرام"""
    if not TOKEN or not CHAT_ID:
        print("  ⚠️ Telegram not configured in .env")
        return False
    
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            print("  ✅ Telegram message sent successfully!")
            return True
        else:
            print(f"  ❌ Telegram error: {response.text}")
            return False
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")
        return False

def send_telegram_test():
    """إرسال رسالة اختبار"""
    message = "✅ *Test message from IoT System!*\n\nThe system is running and alerts are active."
    return send_telegram_alert(message)

if __name__ == "__main__":
    send_telegram_test()