from ftplib import FTP
import ftplib
import logging
import pika
import io
import socket
import time
import json
from os import path

CREDENTIALS_JSON = 'json/credentials.json'
LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'
WORKING_SVG = 'crewe_td_wrk.svg'
MSG_BROKER = None
MB_USER = None
MB_PASS = None
MB_PORT = None
FTP_SERVER = None
FTP_USER = None
FTP_PASS = None

logging.basicConfig(filename='crewe_ftp.log',
                    level=logging.DEBUG,
                    format=LOG_FORMAT,
                    filemode='w')

logger = logging.getLogger()

if path.isfile(CREDENTIALS_JSON):
    with open(CREDENTIALS_JSON, 'r') as js:
        data = json.load(js)
        MSG_BROKER = data['ftp_msg_broker']['server']
        MB_USER = data['ftp_msg_broker']['user_name']
        MB_PASS = data['ftp_msg_broker']['password']
        MB_PORT = int(data['ftp_msg_broker']['port'])
        FTP_SERVER = data['ftp']['server']
        FTP_USER = data['ftp']['user_name']
        FTP_PASS = data['ftp']['password']
else:
    logger.error('Cannot find "{}", cannot continue'.format(CREDENTIALS_JSON))
    exit()

channel = None
credentials = pika.PlainCredentials(MB_USER, MB_PASS)
parameters = pika.ConnectionParameters(MSG_BROKER,
                                       MB_PORT,
                                       '/',
                                       credentials,
                                       heartbeat_interval=90,
                                       retry_delay=5,
                                       socket_timeout=90,
                                       connection_attempts=10)


def create_connection():

    logger.info('Creating connection to message broker...')
    global channel
    connection = pika.BlockingConnection(parameters)
    logger.info('....{}'.format(parameters))
    channel = connection.channel()
    channel.queue_declare(queue='svg')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue='svg')
    logger.info('....success!')


def callback(ch, method, properties, body):

    logger.info('Message received from broker...')
    try:
        with FTP(FTP_SERVER, timeout=10) as ftp:
            logger.info('....{}'.format(ftp.login(user=FTP_USER, passwd=FTP_PASS)))
            bio = io.BytesIO(body)
            logger.info('....{}'.format(ftp.storbinary('STOR ' + WORKING_SVG, bio, 1024)))
            logger.info('....message acknowledge.')
    except socket.error as e:
        logger.error('Socket error: {}'.format(e))
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
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def receive_messages():
    logger.info('Attempting to consume messages')
    global channel
    try:
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(str(e))
        logger.debug('Attempting to reconnect to message broker(pika)...')
        time.sleep(2)
        create_connection()
        receive_messages()
    except Exception as e:
        logger.error(str(e))
        logger.debug('Attempting to reconnect to message broker(other)...')
        time.sleep(2)
        create_connection()
        receive_messages()


def main():
    create_connection()
    receive_messages()


if __name__ == '__main__':
    main()
