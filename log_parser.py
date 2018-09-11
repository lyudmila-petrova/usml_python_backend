import re
import sqlite3
from collections import namedtuple
from geoip import open_database


class LogParser:
    LOG_LINE_RE = r"^.*?(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*?" \
                  r"(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}).*?(?P<url>http.*)$"

    db_name = 'bottom.db'

    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.conn = sqlite3.connect(self.db_name)
        self.cur = self.conn.cursor()
        self.ipdb = open_database('GeoLite2/GeoLite2-Country.mmdb')

        self.log = list()
        self.users = set()
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
