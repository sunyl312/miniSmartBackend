
import smtplib
from email.mime.text import MIMEText
import logging
import ssl
from UserPackage import UserPackage


def auto_mail(this_message, this_subject, t_addr):
    """
    send emails using 
    m  = message
    sbj = subject
    t_addr = to_address  str or list
    """
    f_addr = UserPackage("sunyl_mail")["mail"]
    f_pswd = UserPackage("sunyl_mail")["passwd"]
    f_smtp = "mail.cstnet.cn"
    # f_smtp = "159.226.251.13"
    msg = this_message
    msg['Subject'] = this_subject
    msg['From'] = f_addr
    msg['To'] = ",".join(t_addr) if isinstance(t_addr, list) else t_addr
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.set_ciphers('HIGH:!DH:!aNULL')

    server = smtplib.SMTP_SSL(f_smtp, 465, context=context)
    server.set_debuglevel(0)
    server.login(f_addr, f_pswd)
    server.sendmail(f_addr, t_addr, msg.as_string())
    server.quit()
    logging.info("auto mailing... From:{0} ; To:{1}".format(f_addr, msg['To']))
