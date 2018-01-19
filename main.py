from os import path
import os
from shutil import copyfile
import re
import json
from mq import Mq
import csv
import threading
from queue import Queue
import logging
#import pika
from ftplib import FTP
import socket
import ftplib
import io

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

CREDENTIALS_JSON = 'json/credentials.json'

ORIG_SVG = 'crewe_td.svg'
WORKING_SVG = 'crewe_td_wrk.svg'
SVG_DIR = 'svg'

JSON_FILE = 'crewe_td.json'
JSON_DIR = 'json'

CSV_FILE = 'crewe_td.csv'
CSV_DIR = 'csv'

MSG_BROKER = None
MB_USER = None
MB_PASS = None
MB_PORT = None
FTP_SERVER = None
FTP_USER = None
FTP_PASS = None

LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'

logging.basicConfig(filename='crewe_td.log',
                    level=logging.INFO,
                    format=LOG_FORMAT,
                    filemode='w')

logger = logging.getLogger()

svg_handler = None
SCALE = 16
NUM_OF_BITS = 8
update_lock = threading.Lock()
thread_queue_lock = threading.Lock()
thread_queue = Queue()
berth_list = []

td_matrix = {}
routing_table = {}

if path.isfile(CREDENTIALS_JSON):
    with open(CREDENTIALS_JSON, 'r') as js:
        data = json.load(js)
        MSG_BROKER = data['svg_msg_broker']['server']
        MB_USER = data['svg_msg_broker']['user_name']
        MB_PASS = data['svg_msg_broker']['password']
        MB_PORT = int(data['svg_msg_broker']['port'])
        FTP_SERVER = data['ftp']['server']
        FTP_USER = data['ftp']['user_name']
        FTP_PASS = data['ftp']['password']

else:
    logger.error('Cannot find "{}", cannot continue'.format(CREDENTIALS_JSON))
    exit()

# credentials = pika.PlainCredentials(MB_USER, MB_PASS)
# parameters = pika.ConnectionParameters(MSG_BROKER, MB_PORT, '/', credentials)
# send_message_properties = pika.BasicProperties(expiration='10000', )
#
#
# def send_to_broker(msg):
#
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#     channel.queue_declare(queue='svg')
#
#     channel.basic_publish(exchange='', routing_key='svg', body=msg, properties=send_message_properties)
#     connection.close()


def file_transfer(body):

    try:
        with FTP(FTP_SERVER, timeout=10) as ftp:
            logger.info('....{}'.format(ftp.login(user=FTP_USER, passwd=FTP_PASS)))
            bio = io.BytesIO(body)
            logger.info('....{}'.format(ftp.storbinary('STOR ' + WORKING_SVG, bio, 1024)))
            #logger.info('....message acknowledge.')
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
    else:
        logger.info('....success!')
    #finally:
        #ch.basic_ack(delivery_tag=method.delivery_tag)


class SVGFile:

    @staticmethod
    def check_svg_file():

        """ This method looks for existing files and removes / refreshes as appropriate """

        logger.info('Undertaking some file system house keeping...')
        full_orig_file = path.join(SVG_DIR, ORIG_SVG)
        full_wrk_file = path.join(SVG_DIR, WORKING_SVG)

        try:
            if path.isfile(full_wrk_file):
                os.remove(full_wrk_file)
                logger.info('....deleted old version of {}'.format(full_wrk_file))

            if path.isfile(full_orig_file):
                copyfile(full_orig_file, full_wrk_file)
                logger.info('....created new {}, based on {}'.format(full_wrk_file, full_orig_file))
                return full_wrk_file
            else:
                logger.error('....cannot find {}, cannot continue...'.format(full_orig_file))
                return None
        except Exception as e:
            logger.error('....cannot find {}, cannot continue...'.format(full_orig_file))
            return None


class IncomingMessageHandler:

    @staticmethod
    def make_valid_headcode(description):

        logger.info('Trying to create valid headcode from {}'.format(description))
        letter = re.findall('[A-Z]', description)
        if len(letter) > 1:
            return description
        else:
            numbers = re.findall('[0-9]', description)
            if len(numbers) == 3:
                new_headcode = '{}{}{}{}'.format(numbers[0], letter[0], numbers[1], numbers[2])
                logger.info('....done! {} => {}'.format(description, new_headcode))
                return new_headcode
            else:
                logger.info('....unable to create valid headcode from {}'.format(description))
                return description

    @staticmethod
    def incoming_msg(msg):
        json_msg = json.loads(msg)
        for msg in json_msg:
            for k, v in msg.items():
                if v['area_id'] == 'CE':
                    with thread_queue_lock:
                        if k == 'CA_MSG':  # Berth Step Message
                            description = v['descr']
                            if not re.match(r'^[0-9][A-Z][0-9][0-9]$', description):
                                description = IncomingMessageHandler.make_valid_headcode(description)
                            berth_from = v['from']
                            berth_to = v['to']

                            if berth_from in berth_list or berth_to in berth_list:

                                x = threading.Thread(target=svg_handler.interpose_description, args=(description, berth_to))
                                x.daemon = True
                                thread_queue.put(x)
                                y = threading.Thread(target=svg_handler.clear_berth, args=(berth_from, ))
                                y.daemon = True
                                thread_queue.put(y)

                                logger.info('Berth step: {} from: {}, to: {}'.format(description, berth_from, berth_to))

                        elif k == 'CC_MSG':  # Interpose Message

                            description = v['descr']

                            if not re.match(r'^[0-9][A-Z][0-9][0-9]$', description):
                                description = IncomingMessageHandler.make_valid_headcode(description)

                            berth = v['to']

                            if berth in berth_list:

                                x = threading.Thread(target=svg_handler.interpose_description, args=(description, berth))
                                x.daemon = True
                                thread_queue.put(x)

                                logger.info('Description interpose: {} into berth {}'.format(description, berth))

                        elif k == 'CB_MSG':  # Cancel Message

                            berth = v['from']

                            if berth in berth_list:

                                y = threading.Thread(target=svg_handler.clear_berth, args=(berth,))
                                y.daemon = True
                                thread_queue.put(y)

                                logger.info('Berth cancel: {}'.format(berth))

                        elif k == 'CT_MSG':  # TD Heartbeat
                            logger.info('TD Heartbeat received at {}'.format(v['report_time']))

                        elif k == 'SF_MSG':  # S Class Message

                            x = threading.Thread(target=IncomingMessageHandler.signalling_update, args=(v, ))
                            x.daemon = True
                            thread_queue.put(x)

                        elif k == 'SG_MSG' or k == 'SH_MSG':
                            start_address = v['address']
                            data = re.findall('..', v['data'])

                            for d in data:

                                x = threading.Thread(
                                    target=IncomingMessageHandler.signalling_update,
                                    args=({'address': start_address, 'data': d}, ))
                                x.daemon = True
                                thread_queue.put(x)
                                start_address = int(start_address, 16)
                                start_address += 1
                                start_address = hex(start_address)[2:].zfill(2).upper()

    @staticmethod
    def signalling_update(v):

        address = v['address']  # Get the address from the message
        h_data = v['data']  # Get the message data

        hex_data = bin(int(h_data, SCALE))[2:].zfill(NUM_OF_BITS)  # Convert the message data to 8 bit binary string
        rev_hex_data = hex_data[::-1]  # Reverse the binary string (LSB first)

        for i in range(8):
            bit = int(rev_hex_data[i])  # Get the Nth bit
            if address in td_matrix:  # Check if the address is found within sig_matrix
                for x in range(len(td_matrix[address])):  # Loop though the length of all bits within the address
                    if int(td_matrix[address][x]['bit']) == i:  # Find the correct bit
                        curr_value = td_matrix[address][x]['value']  # Assign the current bit value to a variable
                        if curr_value != bit:  # The received bit value is different the that currently stored
                            td_matrix[address][x]['value'] = bit  # Update the current bit value
                            if str(td_matrix[address][x]['detail']).startswith('R'):

                                if td_matrix[address][x]['value'] == 1:
                                    logger.info('Route Set: {}'.format(td_matrix[address][x]['context']))
                                    svg_handler.set_route(td_matrix[address][x]['detail'])
                                else:
                                    logger.info('Route Cancelled: {}'.format(td_matrix[address][x]['context']))
                                    svg_handler.set_route(td_matrix[address][x]['detail'], False)

                            if td_matrix[address][x]['context'] == 'Signal State':

                                if td_matrix[address][x]['value'] == 1:
                                    logger.info('Signal OFF: {}'.format(format(td_matrix[address][x]['detail'])))
                                    svg_handler.set_signal(td_matrix[address][x]['detail'], False)
                                else:
                                    logger.info('Signal ON: {}'.format(format(td_matrix[address][x]['detail'])))
                                    svg_handler.set_signal(td_matrix[address][x]['detail'], True)

                            if td_matrix[address][x]['context'] == 'TRTS':

                                if td_matrix[address][x]['value'] == 1:
                                    logger.info('{} Operated: {}'.format(td_matrix[address][x]['context'], td_matrix[address][x]['detail']))
                                    svg_handler.show_trts(td_matrix[address][x]['detail'])
                                else:
                                    logger.info('{} Cancelled: {}'.format(td_matrix[address][x]['context'], td_matrix[address][x]['detail']))
                                    svg_handler.clear_trts(td_matrix[address][x]['detail'])

                        break
            else:  # Unknown Address, not found in sig_matrix
                logger.info('Unknown address: {}, with data: {} received'.format(address, h_data))


class SVGHandler:

    svg_file = None
    tree = None
    root = None

    thread_number = 0

    def __init__(self, svg):

        self.svg_file = svg
        self.tree = ET.ElementTree(file=self.svg_file)
        self.root = self.tree.getroot()

    def clear_all_routes(self):

        logger.info('All routes cleared')
        with update_lock:
            for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g[@id="base_layer"]'):
                for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}g[@id="crewe_basic_track_layout"]'):
                    for e in sub_elem.iterfind('.//{http://www.w3.org/2000/svg}path'):
                        style = e.get('style')
                        style = re.sub(r'fill:#[0-9A-Za-z]*', 'fill:#ffffff', style)
                        e.set('style', style)

    def set_route(self, route, set_route=True):

        with update_lock:
            if route in routing_table:
                for trk in routing_table[route]:
                    search_string = './/{{http://www.w3.org/2000/svg}}path[@id="track_{}"]'.format(trk)
                    for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g[@id="base_layer"]'):
                        for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}g[@id="crewe_basic_track_layout"]'):
                            for e in sub_elem.iterfind(search_string):
                                style = e.get('style')
                                if set_route:
                                    style = re.sub(r'fill:#[0-9A-Za-z]*', 'fill:#08db1d', style)
                                else:
                                    style = re.sub(r'fill:#[0-9A-Za-z]*', 'fill:#ffffff', style)
                                e.set('style', style)

    def delete_all_berth_text(self):

        logger.info('All berths cleared')
        with update_lock:
            for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g'):
                if 'Berth' in str(element.attrib):
                    berth_list.append(element.get('id'))
                    for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}tspan'):
                        sub_elem.text = ''

    def interpose_description(self, description, berth):

        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}g[@id="{}"]'.format(berth)
            for elem in self.root.iterfind(search_string):
                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}tspan'):
                    sub_elem.text = description

    def clear_berth(self, berth):

        self.interpose_description('', berth)

    def clear_all_trts(self):

        logger.info('All TRTS Cleared')
        with update_lock:
            search_string = './/{http://www.w3.org/2000/svg}g[@id="trts"]'
            for elem in self.root.iterfind(search_string):

                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}circle'):
                    style = sub_elem.get('style')
                    style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffe355', style)
                    sub_elem.set('style', style)

    def set_signal(self, signal, signal_on=True):

        with update_lock:
            search_string = ".//{{http://www.w3.org/2000/svg}}g[@id='{}']".format(signal)

            for elem in self.root.iterfind(search_string):

                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}ellipse'):

                    style_attrib = sub_elem.get('style')

                    if signal_on:
                        style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#ff0000', style_attrib)

                    else:
                        style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#59f442', style_attrib)

                    sub_elem.set('style', style_attrib)

                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}circle'):

                    style_attrib = sub_elem.get('style')

                    if signal_on:
                        style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#ff0000', style_attrib)
                    else:
                        if str(signal)[2] == '5':
                            style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffffff', style_attrib)
                        else:
                            style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#59f442', style_attrib)

                    sub_elem.set('style', style_attrib)

    def clear_trts(self, trts):

        """ This method indicates that a TRTS has been cancelled """

        logger.info('Clear TRTS: {}'.format(trts))

        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}circle[@id="{}"]'.format(trts)
            for elem in self.root.iterfind(search_string):
                style = elem.get('style')
                style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffe355', style)
                elem.set('style', style)

            logger.info('END: clear_trts')

    def show_trts(self, trts):

        """ This method indicates that a TRTS button has been pressed """

        logger.info('Showing TRTS: {}'.format(trts))
        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}circle[@id="{}"]'.format(trts)
            for elem in self.root.iterfind(search_string):
                style = elem.get('style')
                style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffffff', style)
                elem.set('style', style)

    # @staticmethod
    # def refresh():
    #
    #     if path.isfile(path.join(SVG_DIR, WORKING_SVG)):
    #         with open(path.join(SVG_DIR, WORKING_SVG), 'a'):
    #             threading.Thread(target=svg_handler.upload_to_server()).start()

    def queue_thread(self):

        """ This method is ran as a thread, and starts threads in the thread queue, max of 10 at a time"""

        while True:
            if not thread_queue.empty():
                with thread_queue_lock:
                    if thread_queue.qsize() < 10:
                        mx = thread_queue.qsize()
                    else:
                        mx = 10

                    for x in range(mx):
                        m = thread_queue.get()
                        m.setName(self.thread_number)
                        logger.debug('Starting Thread: {}'.format(m.getName()))
                        m.start()
                        m.join()
                        thread_queue.task_done()
                        self.thread_number += 1
                        logger.debug('Thread {} ended'.format(m.getName()))

                with update_lock:

                    logger.info('Writing SVG...')
                    self.tree.write(path.join(SVG_DIR, WORKING_SVG))
                    logger.info('....success!')

                    logger.info('Sending message to ftp server...')
                    file_transfer(ET.tostring(self.root, method='xml'))

                    logger.info('{} Threads processed on this pass'.format(mx))


class SOPBuilder:

    @staticmethod
    def fill_matrix():

        """ This method reads in the details from the CSV and populates the td_matrix array """

        total_trts = 0
        total_signal = 0
        total_route = 0

        logger.info('Attempting to populate td_matrix...')
        full_file_name = path.join(CSV_DIR, CSV_FILE)
        if path.isfile(full_file_name):
            logger.info('....Found {}'.format(full_file_name))
            with open(full_file_name) as csv_file:
                csv_data = csv.reader(csv_file, delimiter=',')
                logger.info('....Reading {}'.format(full_file_name))

                for row in csv_data:

                    address = str(row[1]).zfill(2)
                    bit = str(row[2])
                    detail = str(row[3])
                    context = str(row[4])
                    value = 0

                    if detail.startswith('P'):  # TRTS
                        total_trts += 1
                    elif detail.startswith('S'):  # Signal State
                        total_signal += 1
                        if 'GL' not in detail:
                            detail = re.findall(r'[0-9]{3,4}', detail)
                            detail = 'CE{}'.format(detail[0])
                        else:
                            detail = re.findall(r'[0-9]{3,4}', detail)
                            detail = 'GL{}'.format(detail[0])
                    elif detail.startswith('R'):  # Route
                        context = str(row[5])
                        total_route += 1
                        if row[12]:
                            tracks = str(row[12]).replace(' ', '')
                            routing_tracks = tracks.split(',')
                            routing_table.update({row[3]: routing_tracks})

                    if address not in td_matrix:
                        td_matrix.update({address: [{'bit': bit, 'detail': detail, 'context': context, 'value': value}]})

                    else:
                        if bit not in td_matrix[address]:

                            td_matrix[address].append({'bit': bit, 'detail': detail, 'context': context, 'value': value})

            logger.info('Summary of {}...'.format(full_file_name))
            logger.info('....{} signals found'.format(total_signal))
            logger.info('....{} routes found'.format(total_route))
            logger.info('....{} TRTS found'.format(total_trts))

    @staticmethod
    def print_json_to_file():

        """ This method prints the contents of the td_matrix to a json file """

        logger.info('Outputting td_matrix to json...')
        file_name = path.join(JSON_DIR, JSON_FILE)
        logger.info('....working with {}'.format(file_name))

        try:
            with open(file_name, 'w') as jf:
                json.dump(td_matrix, jf, indent=2)
        except Exception as e:

            logger.warning('....unable to write json file.')
            return

        finally:
            logger.info('....{} created.'.format(file_name))


if __name__ == '__main__':

    logger.info('Application Start')
    print('Starting Track Worker Safety v2.0...')
    svg_path = SVGFile.check_svg_file()

    if svg_path:

        SOPBuilder.fill_matrix()
        SOPBuilder.print_json_to_file()
        svg_handler = SVGHandler(svg_path)
        svg_handler.delete_all_berth_text()
        svg_handler.clear_all_trts()
        svg_handler.clear_all_routes()
        th = threading.Thread(target=svg_handler.queue_thread)
        th.setName('Queue Manager Thread')
        th.daemon = True
        th.start()
        td_mq = Mq()
        mq_thread = threading.Thread(target=td_mq.connect, args=(IncomingMessageHandler, ))
        mq_thread.setName('MQ Thread')
        mq_thread.start()


