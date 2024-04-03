import smtplib
import re
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import Optional, List

def format_title(s: str) -> str:
    # List of words not to capitalize in titles
    small_words = {"a", "an", "the", "and", "but", "for", "nor", "or", "so", "yet", "at", 
                   "by", "in", "of", "on", "to", "up", "for", "with", "over", "into", "onto", "from"}
    caps_words = {"rmb, cgpi"}
    words = s.split('-')
    title_words = []
    for i, word in enumerate(words):
        if word in caps_words:
            title_words.append(word.upper())
        elif i == 0 or i == len(words) - 1 or word not in small_words:
            title_words.append(word.capitalize())
        else:
            title_words.append(word)

    return ' '.join(title_words)

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
        msg.attach(MIMEText(body, 'html'))

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
        
    def create_email_body(self, email_input: dict, parent_category_urls: dict, updated: dict) -> str:
        """
        Generate email body with categories and URLs. If a category is marked as updated,
        it is noted in the body.

        Parameters:
        email_input (dict): {parent_category: {category: url}, {category: url}...}
        updated (dict): Dictionary with categories that have been updated.

        Returns:
        str: The generated email body.
        """

        header = "<h2 style='font-weight:bold;'>Updates from the official People's Bank of China website</h2>"
        lines = ["<ul>"]  # Start the bullet point list
        for parent_category, category_url_ext_dict in email_input.items():
            parent_url = parent_category_urls[parent_category]
            line = "<li>"
            line += f'{parent_category} (<a href="{parent_url}">link</a>)'
            line += "</li>"
            lines.append(line)
            lines.append("<ul>")
            for category, url_ext in category_url_ext_dict.items():
                formatted_category = re.sub(r'[^a-zA-Z\s]', '', category).lower().replace(' ', '_')
                line = "<li>"
                if formatted_category in updated:
                    line += "<b>(New!)</b> "
                line += f'{category} (<a href="http://www.pbc.gov.cn{url_ext}">view</a>)'
                line += "</li>"
                lines.append(line)
            lines.append("</ul>")
        lines.append("</ul>")  # End the bullet point list
        
        return header + "<br>" + "\n".join(lines)