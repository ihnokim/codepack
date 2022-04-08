from codepack.interfaces.sql_interface import SQLInterface
import pymssql
from typing import Any, Union, Optional


class MSSQL(SQLInterface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.as_dict = None
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> pymssql.Connection:
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

    def query(self, q: Union[str, list], commit: bool = False) -> Optional[list]:
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

    def insert(self, db: str, table: str, commit: bool = False, **kwargs: Any) -> None:
        q = self.make_insert_query(db=db, table=table, **kwargs)
        self.query(q, commit=commit)

    @staticmethod
    def make_insert_query(db: str, table: str, **kwargs: Any) -> str:
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

    def insert_many(self, db: str, table: str, rows: list, commit: bool = False) -> None:
        qs = list()
        for row in rows:
            qs.append(self.make_insert_query(db=db, table=table, **row))
        self.query(qs, commit=commit)

    @staticmethod
    def make_select_query(db: str, table: str, projection: Optional[Union[str, list]] = None,
                          tx_isolation: Optional[str] = None, **kwargs: Any) -> str:
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

    def select(self, db: str, table: str, projection: Optional[Union[str, list]] = None,
               tx_isolation: Optional[str] = None, **kwargs: Any) -> list:
        q = self.make_select_query(db=db, table=table, projection=projection, tx_isolation=tx_isolation, **kwargs)
        return self.query(q)

    def exec(self, db: str, procedure: str, **kwargs: Any) -> list:
        q = self.make_exec_query(db, procedure, **kwargs)
        return self.query(q)

    @staticmethod
    def make_exec_query(db: str, procedure: str, **kwargs: Any) -> str:
        q = 'exec %s.%s' % (db, procedure)
        tokens = list()
        for k, v in kwargs.items():
            tokens.append("@%s = '%s'" % (k, v))
        if len(tokens) > 0:
            q += ' ' + ', '.join(tokens)
        return q

    def delete(self, db: str, table: str, commit: bool = False, **kwargs: Any) -> Optional[list]:
        q = "delete from %s.%s" % (db, table)
        if len(kwargs) > 0:
            q += " where "
            q += self.encode_sql(**kwargs)
            return self.query(q, commit=commit)
        else:
            return None

    def close(self) -> None:
        if not self.closed():
            self.session.close()
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
