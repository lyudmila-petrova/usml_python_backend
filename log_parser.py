import re
import sqlite3
import urllib
from collections import namedtuple
from geoip import open_database
from urllib.parse import urldefrag, parse_qsl


class LogParser:
    LOG_LINE_RE = r"^.*?(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?" \
                  r"(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*?(?P<url>http.*)$"

    KEY_URL_FRAGMENT_RE = r"^\/(?P<fragment>[\w_]+)"

    ACTION_MAIN = 'main'
    ACTION_CATEGORY = 'category'
    ACTION_ADD_TO_CART = 'add_to_cart'
    ACTION_PAY = 'pay'
    ACTION_PAYED = 'payed'

    db_name = 'bottom.db'

    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
        self.ipdb = open_database('GeoLite2/GeoLite2-Country.mmdb')

        self.key_fragment_pattern = re.compile(self.KEY_URL_FRAGMENT_RE)

        self.log = list()
        self.users = set()
        self.orders = set()
        self.current_categories = dict()

        self.init_schema()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def init_schema(self):
        with open('schema.sql') as s:
            self.cur.executescript(s.read())

    def extract_log_lines(self):
        pattern = re.compile(self.LOG_LINE_RE)
        LogLine = namedtuple('LogLine', 'datetime ip url')

        with open('logs.txt', 'r', encoding='utf-8') as f:
            for line in f:
                result = pattern.match(line)
                self.log.append(LogLine(
                    result.group("datetime"),
                    result.group('ip'),
                    result.group('url')
                ))

    def parse(self):
        self.extract_log_lines()

        for log_line in self.log:
            if log_line.ip not in self.users:
                self.create_user(log_line.ip)
                self.users.add(log_line.ip)

            self.fill_action(log_line)

    def create_user(self, ip):
        self.cur.execute("INSERT INTO {0}({1},{2}) VALUES (?, ?);".format('users', 'ip', 'country_code'),
                         (ip, self.country_code_by_ip(ip)))
        self.conn.commit()

    def country_code_by_ip(self, ip):
        res = self.ipdb.lookup(ip)

        country = None
        try:
            country = res.country
        except:
            pass

        return country

    def fill_action(self, log_line):
        f = self.get_key_url_fragment(log_line.url)
        category = None

        if f is None:
            action_type = self.ACTION_MAIN
        elif f == 'cart':
            action_type = self.ACTION_ADD_TO_CART
            params = self.get_url_params(log_line.url)
            order_id = int(params['cart_id'])
            if order_id not in self.orders:
                self.create_order(log_line.ip, order_id)
                self.orders.add(order_id)

            self.create_order_item((order_id, params['goods_id'], params['amount'], self.current_categories[log_line.ip]))
        elif f == 'pay':
            action_type = self.ACTION_PAY
        elif 'success_pay' in f:
            action_type = self.ACTION_PAYED
            order_id = int(f[12:])
            self.pay_order(order_id)
        else:
            action_type = self.ACTION_CATEGORY
            category = f
            self.current_categories[log_line.ip] = category

        self.create_hit((log_line.ip, log_line.datetime, action_type, category))

    def get_key_url_fragment(self, url):
        u = urllib.parse.urlparse(url)
        match = self.key_fragment_pattern.match(u.path)

        if match is None:
            return None
        return match.group('fragment')

    @staticmethod
    def get_url_params(url):
        u = urllib.parse.urlparse(url)
        return dict(parse_qsl(u.query))

    def create_hit(self, hit):
        self.cur.execute('''
            INSERT INTO {0}({1},{2},{3},{4}) VALUES (?, ?, ?, ?);
        '''.format('hits', 'ip', 'datetime', 'action_type', 'product_category'), hit)

        self.conn.commit()

    def create_order(self, ip, order_id):
        self.cur.execute('''
            INSERT INTO {0}({1},{2}) VALUES (?, ?);
        '''.format('orders', 'id', 'ip'), (order_id, ip))

        self.conn.commit()

    def pay_order(self, order_id):
        self.cur.execute('''
             UPDATE {0} SET {1} = (?) WHERE {2} = (?);
        '''.format('orders', 'is_paid', 'id'), (1, order_id))

        self.conn.commit()

    def create_order_item(self, order_item):
        self.cur.execute('''
                    INSERT INTO {0}({1},{2},{3},{4}) VALUES (?, ?, ?, ?);
                '''.format('order_items', 'order_id', 'product_id', 'amount', 'product_category'), order_item)

        self.conn.commit()
