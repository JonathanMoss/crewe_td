from os import path
import os
from shutil import copyfile
import re
import json
from mq import Mq
import csv
import threading
from ftplib import FTP
from queue import Queue
import logging

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

ORIG_SVG = 'crewe_td.svg'
WORKING_SVG = 'crewe_td_wrk.svg'
SVG_DIR = 'svg'

JSON_FILE = 'crewe_td.json'
JSON_DIR = 'json'

CSV_FILE = 'crewe_td.csv'
CSV_DIR = 'csv'

FTP_SERVER = 'ftp.jgm-net.co.uk'
FTP_USER = 'crewe@jgm-net.co.uk'
FTP_PASS = '74!VyJxWK'

LOG_FORMAT = '%(levelname)s %(asctime)s - %(message)s'

svg_handler = None
SCALE = 16
NUM_OF_BITS = 8
update_lock = threading.Lock()
thread_queue_lock = threading.Lock()
thread_queue = Queue()
berth_list = []

td_matrix = {}
routing_table = {}

logging.basicConfig(filename='crewe_td.log',
                    level=logging.DEBUG,
                    format=LOG_FORMAT,
                    filemode='w')
logger = logging.getLogger()


class SVGFile:

    @staticmethod
    def check_svg_file():

        full_orig_file = path.join(SVG_DIR, ORIG_SVG)
        full_wrk_file = path.join(SVG_DIR, WORKING_SVG)

        try:
            if path.isfile(full_wrk_file):
                os.remove(full_wrk_file)
                logger.info('Deleted {}'.format(full_wrk_file))

            if path.isfile(full_orig_file):
                copyfile(full_orig_file, full_wrk_file)
                logger.info('Created new {}, based on {}'.format(full_wrk_file, full_orig_file))
                return full_wrk_file
            else:
                logger.warning('Cannot find {}, Cannot Continue...'.format(full_orig_file))
                return None
        except:
            logger.error('Cannot find {}, Cannot Continue...'.format(full_orig_file))
            return None


class IncomingMessageHandler:

    @staticmethod
    def make_valid_headcode(description):

        letter = re.findall('[A-Z]', description)
        if len(letter) > 1:
            return description
        else:
            numbers = re.findall('[0-9]', description)
            if len(numbers) == 3:
                numbers.sort()
                if int(numbers[2]) in (0, 4, 6, 7, 8):
                    new_headcode = '{}{}{}{}'.format(numbers[2], letter[0], numbers[0], numbers[1])
                elif int(numbers[0]) in (0, 4, 6, 7, 8):
                    new_headcode = '{}{}{}{}'.format(numbers[0], letter[0], numbers[1], numbers[2])
                elif int(numbers[1]) in (0, 4, 6, 7, 8):
                    new_headcode = '{}{}{}{}'.format(numbers[1], letter[0], numbers[0], numbers[2])
                else:
                    return description

                return new_headcode

            else:
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

                        elif k == 'CC_MSG':  # Interpose Message

                            description = v['descr']

                            if not re.match(r'^[0-9][A-Z][0-9][0-9]$', description):
                                description = IncomingMessageHandler.make_valid_headcode(description)

                            berth = v['to']

                            if berth in berth_list:

                                x = threading.Thread(target=svg_handler.interpose_description, args=(description, berth))
                                x.daemon = True
                                thread_queue.put(x)

                        elif k == 'CB_MSG':  # Cancel Message

                            berth = v['from']

                            if berth in berth_list:

                                y = threading.Thread(target=svg_handler.clear_berth, args=(berth,))
                                y.daemon = True
                                thread_queue.put(y)

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
                                    print('Route Set: {}'.format(td_matrix[address][x]['context']))
                                    svg_handler.set_route(td_matrix[address][x]['detail'])
                                else:
                                    print('Route Cancelled: {}'.format(td_matrix[address][x]['context']))
                                    svg_handler.set_route(td_matrix[address][x]['detail'], False)

                            if td_matrix[address][x]['context'] == 'Signal State':

                                if td_matrix[address][x]['value'] == 1:
                                    print('Signal OFF: {}'.format(format(td_matrix[address][x]['detail'])))
                                    svg_handler.set_signal(td_matrix[address][x]['detail'], False)
                                else:
                                    print('Signal ON: {}'.format(format(td_matrix[address][x]['detail'])))
                                    svg_handler.set_signal(td_matrix[address][x]['detail'], True)

                            if td_matrix[address][x]['context'] == 'TRTS':

                                if td_matrix[address][x]['value'] == 1:
                                    print('{} Operated: {}'.format(td_matrix[address][x]['context'], td_matrix[address][x]['detail']))
                                    svg_handler.show_trts(td_matrix[address][x]['detail'])
                                else:
                                    print('{} Cancelled: {}'.format(td_matrix[address][x]['context'], td_matrix[address][x]['detail']))
                                    svg_handler.clear_trts(td_matrix[address][x]['detail'])

                        break
            else:  # Unknown Address, not found in sig_matrix
                print(v)
                pass


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

        logger.info('START: clear_all_routes')
        with update_lock:
            for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g[@id="base_layer"]'):
                for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}g[@id="crewe_basic_track_layout"]'):
                    for e in sub_elem.iterfind('.//{http://www.w3.org/2000/svg}path'):
                        style = e.get('style')
                        style = re.sub(r'fill:#[0-9A-Za-z]*', 'fill:#ffffff', style)
                        e.set('style', style)

            logger.info('END: clear_all_routes')

    def set_route(self, route, set_route=True):

        logger.info('START: set_route')
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
            logger.info('END: set_route')

    def delete_all_berth_text(self):

        logger.info('START: delete_all_berth_text')
        with update_lock:
            for element in self.root.iterfind('.//{http://www.w3.org/2000/svg}g'):
                if 'Berth' in str(element.attrib):
                    berth_list.append(element.get('id'))
                    for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}tspan'):
                        sub_elem.text = ''
            logger.info('END: delete_all_berth_text')

    def interpose_description(self, description, berth):

        logger.info('START: interpose_description')
        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}g[@id="{}"]'.format(berth)
            for elem in self.root.iterfind(search_string):
                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}tspan'):
                    sub_elem.text = description
            logger.info('END: interpose_description')

    def clear_berth(self, berth):

        logger.info('START: clear_berth')
        self.interpose_description('', berth)
        logger.info('END: clear_berth')

    def clear_all_trts(self):

        logger.info('START: clear_all_trts')
        with update_lock:
            search_string = './/{http://www.w3.org/2000/svg}g[@id="trts"]'
            for elem in self.root.iterfind(search_string):

                for sub_elem in elem.iterfind('.//{http://www.w3.org/2000/svg}circle'):
                    style = sub_elem.get('style')
                    style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffe355', style)
                    sub_elem.set('style', style)
            logger.info('END: clear_all_trts')

    def set_signal(self, signal, signal_on=True):

        logger.info('START: set_signal')
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
                        style_attrib = re.sub(r'fill:#[a-z0-9]*', 'fill:#59f442', style_attrib)

                    sub_elem.set('style', style_attrib)

            logger.info('END: set_signal')

    def clear_trts(self, trts):

        logger.info('START: clear_trts')

        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}circle[@id="{}"]'.format(trts)
            for elem in self.root.iterfind(search_string):
                style = elem.get('style')
                style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffe355', style)
                elem.set('style', style)

            logger.info('END: clear_trts')

    def show_trts(self, trts):

        logger.info('START: show_trts')
        with update_lock:
            search_string = './/{{http://www.w3.org/2000/svg}}circle[@id="{}"]'.format(trts)
            for elem in self.root.iterfind(search_string):
                style = elem.get('style')
                style = re.sub(r'fill:#[a-z0-9]*', 'fill:#ffffff', style)
                elem.set('style', style)
            logger.info('END: show_trts')

    @staticmethod
    def upload_to_server():

        try:

            logger.info('START: upload_to_server')
            ftp = FTP(FTP_SERVER)
            print(ftp)
            ftp.set_debuglevel(2)
            url_path = path.join(SVG_DIR, WORKING_SVG)
            with open(url_path, 'rb') as file:
                logger.info('START: Login to FTP Server')
                ftp.login(user=FTP_USER, passwd=FTP_PASS)
                logger.info('END: Login to FTP Server')
                logger.info('START: Upload')
                ftp.storbinary(f'STOR ' + WORKING_SVG, file, 1024)
                logger.info('END: Upload')
                logger.info(ftp.retrlines('LIST'))
                try:
                    ftp.quit()
                finally:
                    pass

        except Exception as e:
            logger.warning('Failed to upload file to server: {}'.format(e))
        finally:
            logger.info('END: upload_to_server')

    @staticmethod
    def refresh():

        logger.info('START: refresh')
        if path.isfile(path.join(SVG_DIR, WORKING_SVG)):
            with open(path.join(SVG_DIR, WORKING_SVG), 'a'):
                threading.Thread(target=svg_handler.upload_to_server()).start()
        logger.info('END: refresh')

    def queue_thread(self):

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
                        print('Starting Thread: {}'.format(m.getName()))
                        m.start()
                        m.join()
                        thread_queue.task_done()
                        self.thread_number += 1
                        print('Thread {} ended'.format(m.getName()))

                with update_lock:
                    logger.info('START: Writing SVG')
                    self.tree.write(path.join(SVG_DIR, WORKING_SVG))
                    logger.info('END: Writing SVG')
                    self.refresh()
                    print('{} Threads Ran'.format(mx))


class SOPBuilder:

    @staticmethod
    def fill_matrix():

        """ This method reads in the details from the CSV and populates the td_matrix array """

        full_file_name = path.join(CSV_DIR, CSV_FILE)
        if path.isfile(full_file_name):
            with open(full_file_name) as csv_file:
                csv_data = csv.reader(csv_file, delimiter=',')
                for row in csv_data:

                    address = str(row[1]).zfill(2)
                    bit = str(row[2])
                    detail = str(row[3])
                    context = str(row[4])
                    value = 0

                    if detail.startswith('P'):  # TRTS
                        pass
                    elif detail.startswith('S'):  # Signal State
                        detail = re.findall(r'[0-9]{3,4}', detail)
                        detail = 'CE{}'.format(detail[0])
                    elif detail.startswith('R'):  # Route
                        context = str(row[5])
                        if row[12]:
                            tracks = str(row[12]).replace(' ', '')
                            routing_tracks = tracks.split(',')
                            routing_table.update({row[3]: routing_tracks})

                    if address not in td_matrix:
                        td_matrix.update({address: [{'bit': bit, 'detail': detail, 'context': context, 'value': value}]})

                    else:
                        if bit not in td_matrix[address]:

                            td_matrix[address].append({'bit': bit, 'detail': detail, 'context': context, 'value': value})

    @staticmethod
    def print_json_to_file():

        file_name = path.join(JSON_DIR, JSON_FILE)
        with open(file_name, 'w') as jf:
            json.dump(td_matrix, jf, indent=2)


if __name__ == '__main__':

    logger.info('Application Start')
    print('Track Worker Safety v2.0')
    svg_path = SVGFile.check_svg_file()
    print(svg_path)

    if svg_path:

        SOPBuilder.fill_matrix()
        SOPBuilder.print_json_to_file()
        svg_handler = SVGHandler(svg_path)
        svg_handler.delete_all_berth_text()
        svg_handler.clear_all_trts()
        svg_handler.clear_all_routes()
        svg_handler.interpose_description('NOGO', '0570')
        svg_handler.interpose_description('BTET', '0202')
        svg_handler.interpose_description('RUST', '0110')
        th = threading.Thread(target=svg_handler.queue_thread)
        th.setName('Queue Manager Thread')
        th.daemon = True
        th.start()
        td_mq = Mq()
        mq_thread = threading.Thread(target=td_mq.connect, args=(IncomingMessageHandler, ))
        mq_thread.setName('MQ Thread')
        mq_thread.start()


