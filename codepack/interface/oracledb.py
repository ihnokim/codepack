from codepack.interface.sql_interface import SQLInterface
import cx_Oracle
from functools import partial


def make_named_row(names, *args):
    if len(names) != len(args):
        raise Exception('len(names) != len(args)')
    return dict(zip(names, args))


class OracleDB(SQLInterface):
    def __init__(self, config, *args, **kwargs):
        super().__init__(config)
        self.as_dict = None
        self.connect(*args, **kwargs)

    def connect(self, *args, **kwargs):
        host, port = self.bind(host=self.config['host'], port=self.config['port'])
        exclude_keys = ['host', 'port']
        if 'service_name' in self.config:
            exclude_keys += ['service_name']
            dsn = cx_Oracle.makedsn(host=host, port=port, service_name=self.config['service_name'])
        else:
            dsn = cx_Oracle.makedsn(host=host, port=port)
        self.as_dict = False
        if 'as_dict' in self.config:
            self.as_dict = self.eval_bool(self.config['as_dict'])
            exclude_keys += ['as_dict']
        if 'as_dict' in kwargs:
            self.as_dict = self.eval_bool(kwargs['as_dict'])
            kwargs = self.exclude_keys(kwargs, keys=['as_dict'])
        _config = self.exclude_keys(self.config, keys=exclude_keys)
        self.session = cx_Oracle.connect(dsn=dsn, *args, **_config, **kwargs)
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
            if cursor.description:
                columns = tuple(c[0] for c in cursor.description)
                if self.as_dict:
                    cursor.rowfactory = partial(make_named_row, columns)
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

    def close(self):
        if not self.closed():
            self.session.close()
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
