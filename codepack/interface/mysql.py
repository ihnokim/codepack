import pymysql
from codepack.interface.abc import SQLInterface
from codepack.interface import isnan
from sshtunnel import SSHTunnelForwarder


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
            self.ssh = SSHTunnelForwarder((self.config['ssh_host'], int(self.config['ssh_port'])),
                                          ssh_password=self.config['ssh_password'],
                                          ssh_username=self.config['ssh_username'],
                                          remote_bind_address=(self.config['host'], int(self.config['port'])))
            self.ssh.start()
            host = '127.0.0.1'
            port = self.ssh.local_bind_port
        else:
            host = self.config['host']
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
                values.append("'%s'" % v)
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

    def call(self, db, procedure, **kwargs):
        q = self.make_call_query(db, procedure, **kwargs)
        return self.query(q)

    @staticmethod
    def make_call_query(db, procedure, **kwargs):
        q = 'call %s.%s' % (db, procedure)
        tokens = list()
        for k, v in kwargs.items():
            tokens.append("@%s := '%s" % (k, v))
        if len(tokens) > 0:
            q += '(%s)' % ', '.join(tokens)
        return q

    def close(self):
        if not self.closed:
            self.conn.close()
            if self.config['ssh_tunneling'] == 'enable' and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self.closed = True
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
