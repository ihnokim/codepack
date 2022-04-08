from codepack.interfaces.sql_interface import SQLInterface
import pymysql
import re
from typing import Any, Union, Optional


class MySQL(SQLInterface):
    def __init__(self, config: dict, *args: Any, **kwargs: Any) -> None:
        super().__init__(config)
        self.connect(*args, **kwargs)

    def connect(self, *args: Any, **kwargs: Any) -> pymysql.connections.Connection:
        host, port = self.bind(host=self.config['host'], port=self.config['port'])
        _config = self.exclude_keys(self.config, keys=['host', 'port'])
        if 'cursorclass' in _config:
            _config['cursorclass'] = self.eval_cursor_object(_config['cursorclass'])
        self.session = pymysql.connect(host=host, port=port, *args, **_config, **kwargs)
        self._closed = False
        return self.session

    @staticmethod
    def eval_cursor_object(cursorclass: Union[str, pymysql.cursors.Cursor]) -> pymysql.cursors.Cursor:
        if type(cursorclass) == str:
            pat = re.compile("\A(pymysql)[.]cursors[.][^.]*(Cursor)\Z")
            assert pat.match(cursorclass), "'cursorclass' should be one of the cursor objects of pymysql"
            return eval(cursorclass)
        else:
            return cursorclass

    def commit(self) -> None:
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()

    def query(self, q: Union[str, list], commit: bool = False) -> Optional[list]:
        assert not self.closed(), "connection is closed"
        columns = None
        rows = None
        try:
            cursor = self.session.cursor()
            cursorclass = cursor.__class__
            if type(q) == str:
                cursor.execute(q)
            elif type(q) == list:
                for qn in q:
                    cursor.execute(qn)
            if cursor.description:
                columns = tuple(c[0] for c in cursor.description)
            rows = cursor.fetchall()
            cursor.close()
            if commit:
                self.commit()
        except Exception as e:
            self.rollback()
            raise e
        if cursorclass in [pymysql.cursors.DictCursor, pymysql.cursors.SSDictCursor]:
            return rows
        else:
            if columns:
                return [columns] + list(rows)
            else:
                return None

    @staticmethod
    def make_insert_query(db: str, table: str, update: bool = True, **kwargs: Any) -> str:
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

    def insert(self, db: str, table: str, update: bool = True, commit: bool = False, **kwargs: Any) -> None:
        q = self.make_insert_query(db=db, table=table, update=update, **kwargs)
        self.query(q, commit=commit)

    def insert_many(self, db: str, table: str, rows: list, update: bool = True, commit: bool = False) -> None:
        qs = list()
        for row in rows:
            qs.append(self.make_insert_query(db=db, table=table, update=update, **row))
        self.query(qs, commit=commit)

    @staticmethod
    def make_select_query(db: str, table: str, projection: Optional[Union[str, list]] = None, **kwargs: Any) -> str:
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

    def select(self, db: str, table: str, projection: Optional[Union[str, list]] = None, **kwargs: Any) -> list:
        q = self.make_select_query(db=db, table=table, projection=projection, **kwargs)
        return self.query(q)

    def delete(self, db: str, table: str, commit: bool = False, **kwargs: Any) -> Optional[list]:
        q = "delete from %s.%s" % (db, table)
        if len(kwargs) > 0:
            q += " where "
            q += self.encode_sql(**kwargs)
            return self.query(q, commit=commit)
        else:
            return None

    def call(self, db: str, procedure: str, **kwargs: Any) -> list:
        q = self.make_call_query(db, procedure, **kwargs)
        return self.query(q)

    @staticmethod
    def make_call_query(db: str, procedure: str, **kwargs: Any) -> str:
        q = 'call %s.%s' % (db, procedure)
        tokens = list()
        for k, v in kwargs.items():
            tokens.append("@%s := '%s'" % (k, v))
        if len(tokens) > 0:
            q += '(%s)' % ', '.join(tokens)
        return q

    def close(self) -> None:
        if not self.closed():
            self.session.close()
            if self.ssh_config and self.ssh is not None:
                self.ssh.stop()
                self.ssh = None
            self._closed = True
