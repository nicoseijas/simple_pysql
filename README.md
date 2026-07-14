# simple_pysql

Tiny, safe CRUD layer over Python's built-in `sqlite3`.

Create, read, update and delete rows with plain dictionaries — no ORM,
no boilerplate, and SQL-injection-safe by default. A single file you drop
into any project.

![License](https://img.shields.io/badge/license-GPLv3-blue)
![Python](https://img.shields.io/badge/python-3.7%2B-blue)
![SQLite](https://img.shields.io/badge/database-SQLite-003B57)
![Tests](https://img.shields.io/badge/tests-55%20passing-brightgreen)
![Dependencies](https://img.shields.io/badge/dependencies-none-success)

```text
   record = {name, phrase}
          │
   simple_pysql.insert / update / delete / count
          │  (identifiers validated, values parameterized)
          ▼
   ┌──────────────────────────┐
   │        sqlite3           │
   │   file.db  or  :memory:  │
   └──────────────────────────┘
```

## Why it exists

Working directly with `sqlite3` means writing the same `INSERT`/`UPDATE`/`DELETE`
strings over and over, remembering to parameterize every value, and juggling
cursors and commits by hand. It's easy to introduce an SQL-injection bug along
the way.

`simple_pysql` wraps that repetition behind four intuitive methods that take
ordinary Python dictionaries. Values are always bound as parameters and table /
column names are validated against a strict whitelist, so injection is closed
off by construction — you focus on the data, not the SQL plumbing.

## Features

- Dictionary-based **Create, Read, Update, Delete**
- **SQL-injection safe**: values are parameterized, identifiers are whitelisted
- **In-memory or file** databases (`:memory:` or `my.db`)
- **`sqlite3.Row`** results — access columns by name
- **Context manager** support (`with … as db:` auto-closes)
- **Generators** for memory-efficient result iteration
- `*_prepare` variants for batching multiple statements before a single commit
- **Zero dependencies** — only the Python standard library

## Installation

From PyPI:

```bash
pip install simple-pysql
```

Or, since it's a single dependency-free file, just clone the repo (or copy
`simple_pysql.py` into your project):

```bash
git clone https://github.com/nicoseijas/simple_pysql
cd simple_pysql
```

Requires Python 3.7+ (uses f-strings and `from __future__ import annotations`).
`pytest` is only required to run the tests.

## Quick Start

```python
from simple_pysql import simple_pysql

# ':memory:' for a throwaway DB, or a path like 'rdr2.db' for a file
with simple_pysql(filename=':memory:', table='rdr2') as db:
    db.query('CREATE TABLE rdr2 (id INTEGER PRIMARY KEY, name TEXT, phrase TEXT)')

    # Create
    db.insert(record=dict(name='Arthur Morgan', phrase='I Gave You All I Had'))
    db.insert(record=dict(name='John Marston', phrase='Remember the name!'))

    # Create many in a single transaction (all records share the same columns)
    db.insert_many(records=[
        dict(name='Sadie Adler', phrase='I trust you'),
        dict(name='Micah Bell', phrase='You lost'),
    ])

    # Update
    db.update(record=dict(name='Jim Milton'), where=dict(id=2))

    # Read
    row = db.get_row('SELECT * FROM rdr2 WHERE id = ?', [1])
    print(row['name'])                       # -> Arthur Morgan

    for r in db.get_results('SELECT * FROM rdr2'):
        print(r['name'], '—', r['phrase'])

    # Delete
    db.delete(where=dict(id=1))
    print(db.count())                        # -> 1
```

### WHERE operators

`where` conditions default to equality (`column: value`). For anything else,
pass a `(operator, value)` tuple. Operators come from a closed whitelist, so
they can't be used for injection:

```python
db.delete(where={'id': ('>', 100)})              # id > 100
db.update(record=dict(active=0),
          where={'name': ('LIKE', 'John%')})     # name LIKE 'John%'
db.delete(where={'id': ('IN', [1, 2, 3])})       # id IN (1, 2, 3)

# Multiple conditions are AND-ed; forms can be mixed:
db.delete(where={'id': ('>=', 10), 'name': 'Arthur Morgan'})
```

Supported operators: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `NOT LIKE`,
`IN`, `NOT IN`, `IS`, `IS NOT`. For `IN` / `NOT IN` the value must be a
non-empty, non-string sequence.

## API

| Method | Purpose |
|---|---|
| `insert(record, table=None)` | Insert a row; returns the new `lastrowid` |
| `insert_many(records, table=None)` | Bulk-insert rows in one transaction; returns the count |
| `update(record, where, table=None)` | Update rows matching `where` |
| `delete(where=None, table=None)` | Delete matching rows (all rows if `where` is omitted) |
| `get_row(sql, params=())` | Fetch a single `sqlite3.Row` (or `None`) |
| `get_results(sql, params=())` | Yield `sqlite3.Row` objects lazily |
| `count()` | Count rows in the current table |
| `query(sql, params=())` | Run any statement and commit (e.g. `CREATE TABLE`) |
| `set_table(table)` | Change the active table |

`insert_prepare` / `update_prepare` / `query_prepare` do the same work **without**
committing, so you can group several writes and call `db.commit()`-equivalent
`insert`/`update` at the end.

## Architecture

```text
your code
   │  dict(record) / dict(where)
   ▼
simple_pysql              ← builds SQL, validates identifiers, binds values
   │  parameterized SQL + params
   ▼
sqlite3 (stdlib)          ← execution + commit
   │
   ▼
SQLite database           ← file.db  or  :memory:
```

Identifiers (table and column names) flow through `_quote_identifier`, which
rejects anything that isn't a plain `^[A-Za-z_][A-Za-z0-9_]*$` name; values
never touch the SQL string and are always passed as bound `?` parameters.

## Technologies

- **Python 3.7+**
- **sqlite3** (Python standard library)
- **pytest** (tests only)

## Project Structure

```text
simple_pysql/
├── simple_pysql.py         # the CRUD module (the whole library)
├── test_simple_pysql.py    # pytest suite
├── README.md
└── LICENSE                 # GPLv3
```

## Testing

```bash
pip install pytest
pytest
```

The suite (31 tests) covers CRUD, the context manager, input validation, and
SQL-injection rejection through table/column identifiers.

## Roadmap

- [x] CRUD with dictionaries
- [x] Parameterized values + identifier whitelisting
- [x] Context manager support
- [x] pytest suite
- [x] `WHERE` operators beyond equality (`>`, `<`, `IN`, `LIKE`, …)
- [x] Bulk `insert_many` helper
- [x] Packaging + PyPI publish workflow (`pip install simple-pysql`)
- [ ] Optional logging hook

## Contributing

Contributions are welcome:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Add or update tests and make sure `pytest` is green
4. Open a Pull Request

## FAQ

**Is it an ORM?**
No. It's a thin, transparent CRUD helper over `sqlite3` — you still write your
own `SELECT`s when you need something beyond the basics.

**Does it protect against SQL injection?**
Yes. Values are always bound as parameters, and table/column names are validated
against a strict whitelist before being placed into the SQL.

**Can I use it with a real file instead of memory?**
Yes — pass `filename='my-db.db'` instead of `':memory:'`.

## Acknowledgements

Inspired by Bill Weinman's `bwDB` module.

## License

Released under the [GNU GPLv3](LICENSE).
