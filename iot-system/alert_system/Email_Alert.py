# alert_system/email_alert.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

load_dotenv()

SENDER = os.getenv('EMAIL_SENDER')
PASSWORD = os.getenv('EMAIL_PASSWORD')
RECEIVER = os.getenv('EMAIL_RECEIVER')
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))

def send_email_alert(subject, body):
    """إرسال تنبيه عبر البريد الإلكتروني"""
    if not SENDER or not PASSWORD or not RECEIVER:
        print("  ⚠️ Email not configured in .env")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = SENDER
        msg['To'] = RECEIVER
        
        # Add body
        msg.attach(MIMEText(body, 'plain'))
        
        # Send email
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SENDER, PASSWORD)
        server.sendmail(SENDER, RECEIVER, msg.as_string())
        server.quit()
        
        print("  ✅ Email sent successfully!")
        return True
        
    except Exception as e:
        print(f"  ❌ Email error: {e}")
        return False

def send_email_test():
    """إرسال رسالة اختبار"""
    subject = "✅ IoT System Test"
    body = "This is a test email from your IoT Anomaly Detection System.\n\nThe system is running and alerts are active."
    return send_email_alert(subject, body)

if __name__ == "__main__":
    send_email_test()