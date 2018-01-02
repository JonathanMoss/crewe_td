from ftplib import FTP
import ftplib
import logging
import pika
import io

FTP_SERVER = 'ftp.jgm-net.co.uk'
FTP_USER = 'crewe@jgm-net.co.uk'
FTP_PASS = '74!VyJxWK'

WORKING_SVG = 'crewe_td_wrk.svg'

LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'

logging.basicConfig(filename='crewe_ftp.log',
                    level=logging.INFO,
                    format=LOG_FORMAT,
                    filemode='w')

logger = logging.getLogger()

MSG_BROKER = '192.168.1.88'
MB_USER = 'crewe_ftp'
MB_PASS = 'crewe_ftp'
MB_PORT = 5672

credentials = pika.PlainCredentials(MB_USER, MB_PASS)
parameters = pika.ConnectionParameters(MSG_BROKER, MB_PORT, '/', credentials)
send_message_properties = pika.BasicProperties(expiration='10000', )

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue='svg')


def callback(ch, method, properties, body):

    logger.info('Message received from broker...')
    try:
        with FTP(FTP_SERVER, timeout=10) as ftp:
            logger.info('....{}'.format(ftp.login(user=FTP_USER, passwd=FTP_PASS)))
            bio = io.BytesIO(body)
            logger.info('....{}'.format(ftp.storbinary('STOR ' + WORKING_SVG, bio, 1024)))
            logger.info('....message acknowledge.')
            ch.basic_ack(delivery_tag=method.delivery_tag)
    except ftplib.error_reply as e:
        logger.error('Unexpected reply received from the server: {}'.format(e))
    except ftplib.error_temp as e:
        logger.error('Temporary error (response codes in the range 400–499): {}'.format(e))
    except ftplib.error_perm as e:
        logger.error('Permanent error (response codes in the range 500–599): {}'.format(e))
    except ftplib.error_proto as e:
        logger.error('Temporary error (response codes in the range 400–499): {}'.format(e))
    except Exception as e:
        logger.error('Non-FTP error: {}'.format(e))


channel.basic_consume(callback, queue='svg')
channel.start_consuming()