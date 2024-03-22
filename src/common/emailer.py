import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional, List

class Emailer():

    def __init__(self, sender: str, gmail_app_password: str):
        self.sender = sender
        self.gmail_app_password = gmail_app_password

    def send_gmail(
            self,
            recipients: list,
            title: str,
            body: str,
            attachments: Optional[List[str]] = None
    ) -> bool:
        """
        Send an email via Gmail with specified details and attachments.

        Parameters:
        sender (str): Your Gmail address.
        recipients (list): List of recipient email addresses.
        title (str): Email subject.
        body (str): Email body.
        attachments (list): List of paths to attachment files.
        """
        msg = MIMEMultipart()
        msg['From'] = self.sender
        msg['To'] = ', '.join(recipients)
        msg['Subject'] = title
        msg.attach(MIMEText(body, 'plain'))

        if attachments:
            # Attach files
            for file_path in attachments:
                with open(file_path, 'rb') as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f"attachment; filename= {file_path.split('/')[-1]}")
                    msg.attach(part)
        # Send email
        try:
            server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
            server.ehlo()
            server.login(self.sender, self.gmail_app_password)
            server.sendmail(self.sender, recipients, msg.as_string())
            server.close()
            print("Email sent successfully!")
            return True
        except Exception as e:
            print(f"Failed to send email: {e}")
            return False