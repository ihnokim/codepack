from codepack.interface.sql_interface import SQLInterface
import pymssql


class MSSQL(SQLInterface):
    def __init__(self, config, *args, **kwargs):
        super().__init__(config)
        self.as_dict = None
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        host, port = self.bind(host=self.config['host'], port=self.config['port'])
        _config = self.exclude_keys(self.config, keys=['host', 'port'])
        self.as_dict = False
        if 'as_dict' in _config:
            self.as_dict = self.eval_bool(_config['as_dict'])
        if 'as_dict' in kwargs:
            self.as_dict = self.eval_bool(kwargs['as_dict'])
        _config['as_dict'] = self.as_dict
        self.session = pymssql.connect(host=host, port=port, *args, **_config, **kwargs)
        self._closed = False
        return self.session

    def query(self, q, commit=False):
        assert not self.closed(), "connection is closed"
        columns = None
        rows = None
        try:
            cursor = self.session.cursor()
            if type(q) == str:
                cursor.execute(q)
            elif type(q) == list:
                for qn in q:
                    cursor.execute(qn)
            if cursor.rowcount == -1:
                if cursor.description:
                    columns = tuple(c[0] for c in cursor.description)
                rows = cursor.fetchall()
            cursor.close()
            if commit:
                self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise e
        if self.as_dict:
            return rows
        else:
            if columns:
                return [columns] + list(rows)
            else:
                return None

    def insert(self, db, table, commit=False, **kwargs):
        q = self.make_insert_query(db=db, table=table, **kwargs)
        self.query(q, commit=commit)

    @staticmethod
    def make_insert_query(db, table, **kwargs):
        columns = kwargs.keys()
        tmp = [kwargs[c] for c in columns]
        values = list()
        for v in tmp:
            if type(v) == str:
                values.append("'%s'" % v)
            elif MSSQL.isnan(v):
                values.append('null')
            else:
                values.append(str(v))
        q = "insert into %s.%s (%s) " % (db, table, ', '.join(columns))
        q += "values (%s)" % ", ".join(values)
        return q

    def insert_many(self, db, table, rows, commit=False):
        qs = list()
        for row in rows:
            qs.append(self.make_insert_query(db=db, table=table, **row))
        self.query(qs, commit=commit)

    @staticmethod
    def make_select_query(db, table, projection=None, tx_isolation=None, **kwargs):
        projection_token = '*'
        if projection:
            if type(projection) == list:
                projection_token = ','.join(projection)
            elif type(projection) == str:
                projection_token = projection
        q = "select %s from %s.%s" % (projection_token, db, table)
        if len(kwargs) > 0:
            q += " where "
            q += MSSQL.encode_sql(**kwargs)
        if tx_isolation:
            q += " with (%s)" % tx_isolation
        return q

    def select(self, db, table, projection=None, tx_isolation=None, **kwargs):
        q = self.make_select_query(db=db, table=table, projection=projection, tx_isolation=tx_isolation, **kwargs)
        return self.query(q)

    def exec(self, db, procedure, **kwargs):
        q = self.make_exec_query(db, procedure, **kwargs)
        return self.query(q)

    @staticmethod
    def make_exec_query(db, procedure, **kwargs):
        q = 'exec %s.%s' % (db, procedure)
        tokens = list()
        for k, v in kwargs.items():
            tokens.append("@%s = '%s'" % (k, v))
        if len(tokens) > 0:
            q += ' ' + ', '.join(tokens)
        return q

    def delete(self, db, table, commit=False, **kwargs):
        q = "delete from %s.%s" % (db, table)
        if len(kwargs) > 0:
            q += " where "
            q += self.encode_sql(**kwargs)
            return self.query(q, commit=commit)
        else:
            return None

    def close(self):
        if not self.closed():
            self.session.close()
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
