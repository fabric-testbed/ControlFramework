#!/usr/bin/env python3
# MIT License
#
# Copyright (c) 2024 FABRIC Testbed
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Komal Thareja (kthare10@renci.org)
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# Function to load the email template from a file and replace placeholders
from typing import Tuple


def load_and_update_template(*, template_path: str, user: str, slice_name: str, hours_left: float) -> Tuple[str, str]:
    """
    Load the Email body from the template
    @param template_path: location of the template
    @param user: user name
    @param slice_name: slice name
    @param hours_left: hours left
    @return: Subject and Body
    """
    with open(template_path, 'r') as template_file:
        email_template = template_file.read()

    # Replace placeholders with actual values
    email_content = email_template.replace('<User>', user) \
        .replace('<slice_name>', slice_name) \
        .replace('<hours_left>', str(hours_left))

    # Extract the subject and the body from the email content
    subject_line = email_content.split('\n')[0].replace("Subject: ", "")
    email_body = "\n".join(email_content.split('\n')[1:])

    return subject_line, email_body


def send_email(*, smtp_config: dict, to_email: str, subject: str, body: str):
    """
    Send Email to a user
    :param smtp_config: SMTP config parameters
    :param to_email:    User's email
    :param subject:     Email subject
    :param body         Email body

    :@raise Exception in case of error
    """
    # Create the message container
    msg = MIMEMultipart()
    msg['From'] = smtp_config.get("from_email")
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.add_header('Reply-To', smtp_config.get("reply_to_email"))

    # Attach the message body
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Establish an SMTP connection and send the email
        server = smtplib.SMTP(smtp_config.get("smtp_server"), smtp_config.get("smtp_port"))
        server.starttls()  # Upgrade to TLS
        server.login(smtp_config.get("smtp_user"), smtp_config.get("smtp_password"))
        server.sendmail(smtp_config.get("from_email"), to_email, msg.as_string())
        # print(f"Email successfully sent to {to_email}")
    finally:
        server.quit()
