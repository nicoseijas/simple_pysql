#!/usr/bin/env python3
# simple_pysql - CRUD module for sqlite3
# Inspired on Bill Weinman's bwDB module

from __future__ import annotations

import re
import sqlite3
from typing import Any, Iterable, Iterator, Mapping, Sequence

__version__ = '0.4'

# SQLite cannot bind identifiers (table/column names) as parameters, so they are
# interpolated into the SQL string. To prevent SQL injection they must be
# validated against a strict whitelist before use.
_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

# WHERE comparison operators are interpolated into the SQL string, so they must
# come from this closed whitelist. The keys are the normalized (upper-cased)
# forms accepted from callers.
_WHERE_OPERATORS = frozenset({
    '=', '!=', '<>', '<', '<=', '>', '>=',
    'LIKE', 'NOT LIKE', 'IN', 'NOT IN', 'IS', 'IS NOT',
})


def _quote_identifier(name: str) -> str:
    """Validate and quote a SQL identifier (table or column name).

    Raises ValueError if the name is not a plain identifier, closing the
    door on injection through table/column names.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(f'Invalid SQL identifier: {name!r}')
    return f'"{name}"'


class simple_pysql:
    def __init__(self, filename: str, table: str = '') -> None:
        """
            db = simple_pysql( filename='filename', table='table_name' )
        """
        self._dbfilename = filename
        self._db = sqlite3.connect(filename)
        # Row Factory accepts the cursor and the original row as a tuple and
        # returns the real result row, allowing access to columns by name.
        self._db.row_factory = sqlite3.Row
        self._table = table

    def __enter__(self) -> 'simple_pysql':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def set_table(self, table: str) -> None:
        """
            If needed, selects a table from database
        """
        self._table = table

    def query(self, sql: str, params: Sequence[Any] = ()) -> None:
        """
           Execute a special query like CREATE TABLE, DROP TABLE, etc
        """
        self._db.execute(sql, params)
        self._db.commit()

    def query_prepare(self, sql: str, params: Sequence[Any] = ()) -> None:
        """
            query_prepare( sql[, params] )
            method for non-select queries *without commit*
                sql is string containing SQL
                params is list containing parameters
            returns nothing
        """
        self._db.execute(sql, params)

    def _resolve_table(self, table: str | None) -> str:
        if table is not None:
            self._table = table
        if not self._table:
            raise ValueError('No table selected')
        return self._table

    def insert_prepare(self, record: Mapping[str, Any], table: str | None = None) -> int:
        """
            Inserts a new record into the database (without commit)
            db.insert_prepare(
                table = 'table',  # Optional parameter
                record = {
                    'column1' : 'value',
                    'column2' : 'value2',
                }
            )
        """
        target = self._resolve_table(table)
        if not record:
            raise ValueError('record must not be empty')

        columns = list(record.keys())
        values = tuple(record.values())

        query = 'INSERT INTO {} ({}) VALUES ({})'.format(
            _quote_identifier(target),
            ', '.join(_quote_identifier(c) for c in columns),
            ', '.join('?' * len(columns)),
        )
        cursor = self._db.execute(query, values)
        return cursor.lastrowid

    def insert(self, record: Mapping[str, Any], table: str | None = None) -> int:
        """
            Inserts a new record into the database and commits.
            db.insert(
                table = 'table',  # Optional parameter
                record = {
                    'column1' : 'value',
                    'column2' : 'value2',
                }
            )
        """
        lastrowid = self.insert_prepare(record, table)
        self._db.commit()
        return lastrowid

    def insert_many(
        self,
        records: Iterable[Mapping[str, Any]],
        table: str | None = None,
    ) -> int:
        """
            Insert many records in a single transaction (one commit).
            All records must share the same columns.
            db.insert_many(
                table = 'table',  # Optional parameter
                records = [
                    { 'column1' : 'a', 'column2' : 'b' },
                    { 'column1' : 'c', 'column2' : 'd' },
                ]
            )
            returns the number of inserted rows.
        """
        target = self._resolve_table(table)
        records = list(records)
        if not records:
            raise ValueError('records must not be empty')

        columns = list(records[0].keys())
        if not columns:
            raise ValueError('records must not be empty')
        column_set = set(columns)

        rows = []
        for record in records:
            if set(record.keys()) != column_set:
                raise ValueError('all records must have the same columns')
            rows.append(tuple(record[c] for c in columns))

        query = 'INSERT INTO {} ({}) VALUES ({})'.format(
            _quote_identifier(target),
            ', '.join(_quote_identifier(c) for c in columns),
            ', '.join('?' * len(columns)),
        )
        self._db.executemany(query, rows)
        self._db.commit()
        return len(rows)

    def _build_where(self, where: Mapping[str, Any]) -> tuple[str, list[Any]]:
        """Build a parameterized WHERE clause from a mapping.

        Each entry is either ``column: value`` (equality) or
        ``column: (operator, value)`` where operator comes from a closed
        whitelist. Column names are validated/quoted and operators are
        whitelisted; values are always bound as parameters.
        """
        conditions: list[str] = []
        values: list[Any] = []

        for column, criterion in where.items():
            col = _quote_identifier(column)

            if isinstance(criterion, tuple):
                if len(criterion) != 2:
                    raise ValueError(
                        'WHERE criterion tuple must be (operator, value), '
                        f'got {criterion!r}'
                    )
                operator, value = criterion
                operator = operator.upper() if isinstance(operator, str) else operator
                if operator not in _WHERE_OPERATORS:
                    raise ValueError(f'Unsupported WHERE operator: {operator!r}')

                if operator in ('IN', 'NOT IN'):
                    if isinstance(value, (str, bytes)) or not isinstance(
                        value, (list, tuple, set)
                    ):
                        raise ValueError(
                            f'{operator} requires a non-string sequence, '
                            f'got {value!r}'
                        )
                    value = list(value)
                    if not value:
                        raise ValueError(f'{operator} requires a non-empty sequence')
                    placeholders = ', '.join('?' * len(value))
                    conditions.append(f'{col} {operator} ({placeholders})')
                    values.extend(value)
                else:
                    conditions.append(f'{col} {operator} ?')
                    values.append(value)
            else:
                conditions.append(f'{col} = ?')
                values.append(criterion)

        clause = ' AND '.join(conditions)
        return f'WHERE {clause}', values

    def update_prepare(
        self,
        record: Mapping[str, Any],
        where: Mapping[str, Any],
        table: str | None = None,
    ) -> None:
        """
            Update an existing record in the database (without commit)
            db.update_prepare(
                table = 'table',  # Optional parameter
                record = { 'column1' : 'value' },
                where = { 'id' : 1 }
            )
        """
        target = self._resolve_table(table)
        if not record:
            raise ValueError('record must not be empty')
        if not where:
            raise ValueError('where must not be empty')

        set_columns = list(record.keys())
        set_values = list(record.values())
        set_clause = ', '.join(
            f'{_quote_identifier(c)} = ?' for c in set_columns
        )
        where_clause, where_values = self._build_where(where)

        query = 'UPDATE {} SET {} {}'.format(
            _quote_identifier(target),
            set_clause,
            where_clause,
        )
        self._db.execute(query, set_values + where_values)

    def update(
        self,
        record: Mapping[str, Any],
        where: Mapping[str, Any],
        table: str | None = None,
    ) -> None:
        """
            Update an existing record in the database and commits.
            db.update(
                table = 'table',  # Optional parameter
                record = { 'column1' : 'value' },
                where = { 'id' : 1 }
            )
        """
        self.update_prepare(record, where, table)
        self._db.commit()

    def get_row(self, query: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        """
            Returns a single row as a Row Factory
            db.get_row('SELECT * FROM table WHERE id = ?', ['1'])
        """
        row = self._db.execute(query, params)
        return row.fetchone()

    def get_results(self, query: str, params: Sequence[Any] = ()) -> Iterator[sqlite3.Row]:
        """
            Returns multiple rows as a Row Factory
            db.get_results('SELECT * FROM table')
        """
        rows = self._db.execute(query, params)
        for row in rows:
            yield row

    def delete(self, where: Mapping[str, Any] | None = None, table: str | None = None) -> None:
        """
            Delete the selected rows in the table
            db.delete(
                table = 'table',  # Optional parameter
                where = { 'id' : 1 }
            )
        """
        target = self._resolve_table(table)

        if where:
            where_clause, values = self._build_where(where)
            query = f'DELETE FROM {_quote_identifier(target)} {where_clause}'
        else:
            values = []
            query = f'DELETE FROM {_quote_identifier(target)}'

        self._db.execute(query, values)
        self._db.commit()

    def count(self) -> int:
        """
            Count the records in a table
        """
        target = self._resolve_table(None)
        query = f'SELECT COUNT(*) FROM {_quote_identifier(target)}'
        cursor = self._db.execute(query)
        return cursor.fetchone()[0]

    @property
    def filename(self) -> str:
        return self._dbfilename

    def close(self) -> None:
        self._db.close()

    @staticmethod
    def version() -> str:
        return __version__
