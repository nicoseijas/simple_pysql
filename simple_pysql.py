#!/usr/bin/env python3
# simple_pysql - CRUD module for sqlite3
# Inspired on Bill Weinman's bwDB module

import sqlite3

__version__ = '0.1'

class simple_pysql:
    def __init__(self, **kwargs) :
        """
            db = simple_pysql( table='table_name', filename='filename' )
        """
        self._filename = kwargs.get('filename')
        self._table = kwargs.get('table', '')

    def set_table(self, table) :
        """
            If needed, selects a table from database
        """
        self._table = table

    def query(self, sql, params=()):
        """
           Execute a special query like CREATE TABLE, DROP TABLE, etc 
        """
        self._db.execute(sql, params)
        self._db.commit()

    def query_prepare(self, sql, params=()):
        """
            query_prepare( sql[, params] )
            method for non-select queries *without commit*
                sql is string containing SQL
                params is list containing parameters
            returns nothing
        """
        self._db.execute(sql, params)

    def insert_prepare(self, **kwargs) :
        """
            Inserts a new record into the database
            db.insert_prepare(
                table = 'table' # Optional parameter
                record = {
                    'record1' : 'value',
                    'record2' : 'value2',
                    'record3' : 'value3'
                }
            )
        """
        if 'table' in kwargs:
            self._table = kwargs['table']

        columns = kwargs['record'].keys()
        values = tuple(kwargs['record'].values())
        print(values)

        query = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self._table,
            ', '.join(columns),
            ', '.join('?' * len(columns))
        )
        id = self._db.execute(query, values)
        return id.lastrowid

    def insert(self, **kwargs) :
        """
            Inserts a new record into the database
            db.insert_prepare(
                table = 'table' # Optional parameter
                record = {
                    'record1' : 'value',
                    'record2' : 'value2',
                    'record3' : 'value3'
                }
            )
        """
        lastrowid = self.insert_prepare(**kwargs)
        self._db.commit()
        return lastrowid

    def update_prepare(self, **kwargs) :
        """
            Update an existing record in the database
            db.update_prepare(
                table = 'table', # Optional parameter
                record = {
                    'record1' : 'value',
                    'record2' : 'value2',
                    'record3' : 'value3'
                },
                where = {
                    'where1' : 'value'
                }
            )
        """
        if 'table' in kwargs:
            self._table = kwargs['table']

        columns = list(kwargs['record'].keys())
        values = list(kwargs['record'].values())
        where_columns = list(kwargs['where'].keys())
        where_values = list(kwargs['where'].values())

        query = 'UPDATE {} SET {} {}'.format(
            self._table,
            ', '.join(map(lambda s: '{} = ?'.format(s), columns)),
            ' '.join(map(lambda w: 'WHERE {} = ?'.format(w) if where_columns.index(w) == 0 else 'AND {} = ?'.format(w), where_columns ))
        )
        print(query)
        self._db.execute(query, values + where_values)

    def update(self, **kwargs) :
        """
            Update an existing record in the database
            db.update_prepare(
                table = 'table', # Optional parameter
                record = {
                    'record1' : 'value',
                    'record2' : 'value2',
                    'record3' : 'value3'
                },
                where = {
                    'where1' : 'value'
                }
            )
        """
        self.update_prepare(**kwargs)
        self._db.commit()

    def get_row(self, query, params=()) :
        """
            Returns a single row as a Row Factory
            db.get_row('SELECT * FROM table WHERE id = ?', ['1'])
        """
        row = self._db.execute(query, params)
        return row.fetchone()

    def get_results(self, query, params=()) :
        """
            Returns multiple rows as a Row Factory
            db.get_results('SELECT * FROM table')
        """
        rows = self._db.execute(query, params)
        for row in rows:
            yield row
    
    def delete(self, **kwargs) :
        """
            Delete the selected rows in the table
            db.delete(
                table = 'table' # Optional parameter
                where = {
                    'id' : 1
                })
        """
        where = kwargs.get('where')

        if 'table' in kwargs:
            self._table = kwargs['table']

        values = ''

        if where :
            columns = list(kwargs['where'].keys())
            values = list(kwargs['where'].values())
            where = ' '.join(map(lambda w: 'WHERE {} = ?'.format(w) if columns.index(w) == 0 else 'AND {} = ?'.format(w), columns ))

        query = 'DELETE FROM {} {}'.format(
            self._table,
            where
        )

        self._db.execute(query, values)
        self._db.commit()

    def count(self) :
        """
            Count the records in a table
        """
        query = f'SELECT COUNT(*) FROM {self._table}'
        count = self._db.execute(query)
        return count.fetchone()[0]

    @property
    def _filename(self) :
        return self._dbfilename

    @_filename.setter
    def _filename(self, fn) :
        self._dbfilename = fn
        self._db = sqlite3.connect(fn)
        """
            Row Factory accepts the cursor and the original row as a tuple 
            and will return the real result row. This way, you can implement 
            more advanced ways of returning results, such as returning 
            an object that can also access columns by name.
        """
        self._db.row_factory = sqlite3.Row

    @_filename.deleter
    def _filename(self) :
        self.close()

    def close(self) :
        self._db.close()
        del self._dbfilename

    def version(self=None) :
        return __version__
