import imaplib
import email

iamp_url = 'imap.gmail.com'

def get_body():
    conn = imaplib.IMAP4_SSL(iamp_url, 993)
    conn.login(email.email_user(), email.email_senha())
    conn.select('inbox')

get_body()