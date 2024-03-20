import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

################################################################################
# 
################################################################################
def send_email(sender, recipients, title, body, attachments):
    """
    Send an email with the specified details and attachments.

    Parameters:
    sender (str): The email address of the sender.
    recipients (list): A list of email addresses to send the email to.
    title (str): The subject line of the email.
    body (str): The main body text of the email.
    attachments (list): A list of file paths to attach to the email.
    """
    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Subject'] = title
    
    # Attach the email body
    msg.attach(MIMEText(body, 'plain'))
    
    # Attach any files
    for file in attachments:
        with open(file, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename= {file.split('/')[-1]}")
            msg.attach(part)
    
    # Log in to the SMTP server and send the email
    try:
        # Update with your SMTP server details
        server = smtplib.SMTP('your.smtp.server', 587)  # Change 'your.smtp.server' and port
        server.starttls()
        server.login('your_email_username', 'your_email_password')  # Change to your email username and password
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

"""
# Example usage
sender = 'your_work_email@example.com'
recipients = ['recipient1@example.com', 'recipient2@example.com']
title = 'Test Email from Python'
body = 'This is a test email sent from Python.'
attachments = ['/path/to/attachment1.pdf', '/path/to/attachment2.jpg']

send_email(sender, recipients, title, body, attachments)
"""

################################################################################
# 
################################################################################
def send_email_via_gmail(
        sender: str,
        recipients: list,
        title: str,
        body: str,
        app_password: str,
        attachments=None
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
    msg['From'] = sender
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
        server.login(sender, app_password)
        server.sendmail(sender, recipients, msg.as_string())
        server.close()
        print("Email sent successfully!")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

