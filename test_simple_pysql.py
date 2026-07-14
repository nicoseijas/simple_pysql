"""Tests for the simple_pysql CRUD module."""

import sqlite3

import pytest

from simple_pysql import __version__, _quote_identifier, simple_pysql

RECORDS = [
    dict(name='Arthur Morgan', phrase='I Gave You All I Had'),
    dict(name='John Marston', phrase='John Marston! Remember the name!'),
    dict(name='Sadie Adler', phrase="You're the only one of these fools that I trust"),
]


@pytest.fixture
def db():
    """An in-memory database with a populated 'rdr2' table."""
    conn = simple_pysql(filename=':memory:', table='rdr2')
    conn.query(
        'CREATE TABLE rdr2 (id INTEGER PRIMARY KEY, name TEXT, phrase TEXT)'
    )
    for record in RECORDS:
        conn.insert(record=record)
    yield conn
    conn.close()


# --- construction -----------------------------------------------------------

def test_init_sets_table_and_connection():
    conn = simple_pysql(filename=':memory:', table='t')
    assert conn._table == 't'
    assert conn.filename == ':memory:'
    conn.close()


def test_context_manager_closes_connection():
    with simple_pysql(filename=':memory:', table='t') as conn:
        conn.query('CREATE TABLE t (id INTEGER PRIMARY KEY)')
    with pytest.raises(sqlite3.ProgrammingError):
        conn.query('SELECT 1')


def test_set_table():
    conn = simple_pysql(filename=':memory:')
    conn.set_table('other')
    assert conn._table == 'other'
    conn.close()


# --- insert -----------------------------------------------------------------

def test_insert_returns_lastrowid(db):
    new_id = db.insert(record=dict(name='Micah Bell', phrase='You lost'))
    assert new_id == 4


def test_insert_persists_record(db):
    db.insert(record=dict(name='Micah Bell', phrase='You lost'))
    row = db.get_row('SELECT * FROM rdr2 WHERE id = ?', [4])
    assert row['name'] == 'Micah Bell'


def test_insert_with_table_argument():
    conn = simple_pysql(filename=':memory:')
    conn.query('CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)')
    conn.insert(record=dict(name='Dutch'), table='people')
    assert conn._table == 'people'
    assert conn.count() == 1
    conn.close()


def test_insert_empty_record_raises(db):
    with pytest.raises(ValueError):
        db.insert(record={})


def test_insert_without_table_raises():
    conn = simple_pysql(filename=':memory:')
    with pytest.raises(ValueError):
        conn.insert(record=dict(name='x'))
    conn.close()


# --- update -----------------------------------------------------------------

def test_update_changes_record(db):
    db.update(record=dict(name='Jim Milton'), where=dict(id=2))
    row = db.get_row('SELECT name FROM rdr2 WHERE id = ?', [2])
    assert row['name'] == 'Jim Milton'


def test_update_multiple_where_conditions(db):
    db.update(
        record=dict(phrase='changed'),
        where=dict(id=1, name='Arthur Morgan'),
    )
    row = db.get_row('SELECT phrase FROM rdr2 WHERE id = ?', [1])
    assert row['phrase'] == 'changed'


def test_update_empty_record_raises(db):
    with pytest.raises(ValueError):
        db.update(record={}, where=dict(id=1))


def test_update_empty_where_raises(db):
    with pytest.raises(ValueError):
        db.update(record=dict(name='x'), where={})


# --- delete -----------------------------------------------------------------

def test_delete_removes_matching_row(db):
    db.delete(where=dict(id=1))
    assert db.count() == 2
    assert db.get_row('SELECT * FROM rdr2 WHERE id = ?', [1]) is None


def test_delete_without_where_clears_table(db):
    db.delete()
    assert db.count() == 0


# --- reads ------------------------------------------------------------------

def test_get_row_returns_row_by_name(db):
    row = db.get_row('SELECT * FROM rdr2 WHERE id = ?', [1])
    assert row['name'] == 'Arthur Morgan'


def test_get_row_missing_returns_none(db):
    assert db.get_row('SELECT * FROM rdr2 WHERE id = ?', [999]) is None


def test_get_results_yields_all_rows(db):
    rows = list(db.get_results('SELECT * FROM rdr2 ORDER BY id'))
    assert [r['name'] for r in rows] == [rec['name'] for rec in RECORDS]


def test_count(db):
    assert db.count() == 3


def test_count_without_table_raises():
    conn = simple_pysql(filename=':memory:')
    with pytest.raises(ValueError):
        conn.count()
    conn.close()


# --- security: SQL injection through identifiers ---------------------------

@pytest.mark.parametrize(
    'bad',
    [
        'rdr2; DROP TABLE rdr2;--',
        'rdr2"; DROP TABLE rdr2;--',
        '1invalid',
        'has space',
        '',
        'name)',
    ],
)
def test_quote_identifier_rejects_malicious_names(bad):
    with pytest.raises(ValueError):
        _quote_identifier(bad)


def test_quote_identifier_accepts_valid_names():
    assert _quote_identifier('rdr2') == '"rdr2"'
    assert _quote_identifier('_col1') == '"_col1"'


def test_insert_rejects_injection_via_table(db):
    with pytest.raises(ValueError):
        db.insert(record=dict(name='x'), table='rdr2; DROP TABLE rdr2;--')


def test_insert_rejects_injection_via_column(db):
    with pytest.raises(ValueError):
        db.insert(record={'name); DROP TABLE rdr2;--': 'x'})


def test_delete_rejects_injection_via_where_column(db):
    with pytest.raises(ValueError):
        db.delete(where={'id = 1 OR 1=1;--': 1})


def test_values_are_not_interpreted_as_sql(db):
    # A malicious value must be stored literally, not executed.
    payload = "Robert'); DROP TABLE rdr2;--"
    db.insert(record=dict(name=payload))
    row = db.get_row('SELECT name FROM rdr2 WHERE id = ?', [4])
    assert row['name'] == payload
    assert db.count() == 4  # table survived


# --- misc -------------------------------------------------------------------

def test_version_matches_module():
    assert simple_pysql.version() == __version__
