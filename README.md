# Introduction

Simple PySQL it's a powerful tool to Create, Read, Update and Delete (CRUD) information from a SQL database. It's part of a light framework based on Python that I'm creating. Any feedback would be welcomed

# Usage

It has the standard funcionality of a CRUD module or class and it's very intuitive to use

## Making the connection

```Python
fn = ':memory:'  # in-memory database
t = 'rdr2'
# Creating or reading the database
# In this case, the database is created in the memory,
# so if you wanna create a file, just write for example:
# fn = 'my-db.db
db = simple_pysql(filename=fn, table=t)
```

## Insert a new record

```python
# Inserting new information
recs = [
    dict(name='Arthur Morgan', phrase='I Gave You All I Had'),
    dict(name='John Marston', phrase='John Marston! Remember the name!'),
    dict(name='Sadie Adler', phrase='You\'re the only one of these fools that I trust')
]
    
for r in recs:
        db.insert(record = r)
        
# Also, you can do it this way
db.insert(record = {
    'name' : 'Micah Bell',
    'phrase' : 'Oh, Black Lung, you ain\'t gonna reach that gun. You ain\'t. You\'ve lost, my sick friend. You lost'
})
```

## Update a record

```python
db.update(record = dict(name='Jim Milton', phrase='Jim Milton Rides, Again?'), where = dict(id = 2))

# Or this way

db.update(record = {
    'name' : 'Jim Milton', 
    'phrase' : 'Jim Milton Rides, Again?'
}, 
    where = {
      'id' : 2
    })
```

## Delete a record 

```python
print('Deleting an element... {} is gone'.format(list(db.get_row("SELECT * FROM rdr2 WHERE id = ?", [4]))[1]))
db.delete(where = {
    'id' : 4
})
```
