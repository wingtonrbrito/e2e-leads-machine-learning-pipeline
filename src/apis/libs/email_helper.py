
from flask_mail import Message, Mail
from libs.flask_app import FlaskApp
import os
from libs.shared.config import Config


class EmailHelper:
    mail_default_sender = 'noreply-recommendation-engine@client.com'
    recipients = ['bd480e20-1fac-47b4-9177-edc195918121+dtm@alert.victorops.com']

    @classmethod
    def mail_config(cls, app):
        try:
            sendgrid_api = Config.secret(os.environ['ENV'], 'sendgrid_api_key')
            app.config['SECRET_KEY'] = 'top-secret!'
            app.config['MAIL_SERVER'] = 'smtp.sendgrid.net'
            app.config['MAIL_PORT'] = 587
            app.config['MAIL_USE_TLS'] = True
            app.config['MAIL_USERNAME'] = 'apikey'
            app.config['MAIL_PASSWORD'] = sendgrid_api
            app.config['MAIL_DEFAULT_SENDER'] = EmailHelper.mail_default_sender
            app.mail = Mail(app)
            app.mail.recipients = EmailHelper.recipients
        except Exception as e:
            print('\n\n\n exception configuring email')
            print(e)

    @classmethod
    def compose_message(cls, issue, subject):
        mail = FlaskApp.app().mail
        msg = Message(f'{subject} critical', recipients=mail.recipients)
        msg.body = (issue)
        msg.html = (f"""
                    <h1>{subject} critical</h1>
                    <p>{issue}
                    <b>. Check Cloud Run logs</b>!</p>""")
        return msg

    @classmethod
    def report_issue(cls, issue, subject='Apis cloudrun'):
        print('\n\nsending email.....')
        try:
            mail = FlaskApp.app().mail
            mail.send(cls.compose_message(issue, subject))
            print('\n\n\n email sent....')
        except Exception as e:
            print('\n\n\n exception sending email')
            print(e)
