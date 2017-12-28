import stomp
import time

_USER_N = 'joth.moss@googlemail.com'
_PASS_W = 'Chester42!'
_TOPIC = '/topic/TD_LNW_C_SIG_AREA'
_VHOST = 'datafeeds.networkrail.co.uk'
_PORT = 61618


class MqListener(stomp.ConnectionListener):

    def __init__(self, conn, msg_handler):
        self.conn = conn
        self.msg_handler = msg_handler

    def on_error(self, headers, message):
        print(message)

    def on_message(self, headers, message):
        self.msg_handler.incoming_msg(message)

    def on_disconnected(self):
        Mq.connect_and_subscribe()


class Mq:

    _conn = None

    @staticmethod
    def connect_and_subscribe():

        Mq._conn.start()
        Mq._conn.connect(_USER_N, _PASS_W, wait=True, headers={'client-id': _USER_N})
        Mq._conn.subscribe(_TOPIC, id=_USER_N, ack='auto', headers={'activemq.subscriptionName': _USER_N, })

    @staticmethod
    def _get_connection(msg_handler):

        Mq._conn = stomp.Connection(host_and_ports=[(_VHOST, _PORT)],
                                    keepalive=False,
                                    vhost=_VHOST)

        Mq._conn.set_listener('', MqListener(Mq._conn, msg_handler))

    @staticmethod
    def connect(msg_handler):
        if Mq._conn is None:
            Mq._get_connection(msg_handler)

        Mq.connect_and_subscribe()

        while Mq._conn.is_connected():
            time.sleep(1)

        print('Disconnected')

