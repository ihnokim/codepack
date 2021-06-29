import pymysql
from codepack.interface.abc import SQLInterface


class MySQL(SQLInterface):
    def __init__(self, config, ssh_config=None, **kwargs):
        super().__init__()
        self.conn = None
        self.connect(config=config, ssh_config=ssh_config, **kwargs)

    def connect(self, config, ssh_config=None, **kwargs):
        self.config = config
        host, port = self.set_sshtunnel(host=self.config['host'], port=self.config['port'], ssh_config=ssh_config)
        _config = self.exclude_keys(self.config, keys=['host', 'port'])
        self.conn = pymysql.connect(host=host, port=port,
                                    cursorclass=pymysql.cursors.DictCursor, **_config, **kwargs)
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
            elif MySQL.isnan(v):
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

    @staticmethod
    def make_select_query(db, table, projection=None, **kwargs):
        projection_token = '*'
        if projection:
            if type(projection) == list:
                projection_token = ','.join(projection)
            elif type(projection) == str:
                projection_token = projection
        q = "select %s from %s.%s" % (projection_token, db, table)
        if len(kwargs) > 0:
            q += " where "
            q += MySQL.encode_sql(**kwargs)
        return q

    def select(self, db, table, projection=None, **kwargs):
        q = self.make_select_query(db=db, table=table, projection=projection, **kwargs)
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
            tokens.append("@%s := '%s'" % (k, v))
        if len(tokens) > 0:
            q += '(%s)' % ', '.join(tokens)
        return q

    def close(self):
        if not self.closed:
            self.conn.close()
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self.closed = True
