import pymysql
from codepack.interface.abc import SQLInterface
from sshtunnel import SSHTunnelForwarder
import numpy as np


class MySQL(SQLInterface):
    def __init__(self, config, **kwargs):
        super().__init__(config)
        self.config = None
        self.ssh = None
        self.conn = None
        self.closed = True
        self.connect(config, **kwargs)

    def connect(self, config, **kwargs):
        self.config = config
        if self.config['ssh_tunneling'] == 'enable':
            ssh = SSHTunnelForwarder((self.config['ssh_host'], int(self.config['ssh_port'])),
                                     ssh_password=self.config['ssh_password'],
                                     ssh_username=self.config['ssh_username'],
                                     remote_bind_address=(self.config['host'], int(self.config['port'])))
            ssh.start()
            host = '127.0.0.1'
            port = ssh.local_bind_port
        else:
            host = self.config['ip']
            port = int(self.config['port'])

        self.conn = pymysql.connect(host=host, port=port,
                                    user=self.config['user'], passwd=self.config['passwd'],
                                    cursorclass=pymysql.cursors.DictCursor, **kwargs)
        self.closed = False
        return self.conn

    def query(self, q, commit=False):
        ret = None

        try:
            cursor = self.conn.cursor()
            if type(q) == str:
                cursor.execute(q)
            elif type(q) == list:
                for qn in q:
                    cursor.execute(qn)
            ret = cursor.fetchall()
            cursor.close()
            if commit:
                self.conn.commit()
        except Exception as e:
            print(e)
            self.conn.rollback()
        return ret

    @staticmethod
    def make_insert_query(db, table, update=True, **kwargs):
        columns = kwargs.keys()
        tmp = [kwargs[c] for c in columns]
        values = list()
        for v in tmp:
            if type(v) == str:
                values.append("'" + v + "'")
            elif isnan(v):
                values.append('null')
            else:
                values.append(str(v))
        if update:
            q = "replace"
        else:
            q = "insert"
        q += " into %s.%s (%s) " % (db, table, ', '.join(columns))
        q += "values (%s)" % ", ".join(values)
        return q

    def insert(self, db, table, update=True, commit=False, **kwargs):
        q = self.make_insert_query(db=db, table=table, update=update, **kwargs)
        self.query(q, commit=commit)

    def insert_many(self, db, table, rows, update=True, commit=False):
        qs = list()
        for row in rows:
            qs.append(self.make_insert_query(db=db, table=table, update=update, **row))
        self.query(qs, commit=commit)
    
    def select(self, db, table, **kwargs):
        q = "select * from %s.%s" % (db, table)
        if len(kwargs) > 0:
            q += " where "
            q += self.encode_sql(**kwargs)
        return self.query(q)
    
    def delete(self, db, table, commit=False, **kwargs):
        q = "delete from %s.%s" % (db, table)
        if len(kwargs) > 0:
            q += " where "
            q += self.encode_sql(**kwargs)
            return self.query(q, commit=commit)
        else:
            return None
    
    def close(self):
        if not self.closed:
            self.conn.close()
            if self.config['ssh_tunneling'] == 'enable' and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self.closed = True
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def isnan(value):
    ret = False
    try:
        ret = np.isnan(value)
    except Exception:
        pass
    finally:
        return ret
