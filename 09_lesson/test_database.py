from sqlalchemy import create_engine, inspect,text

db_connection_string = "postgresql://postgres:postgres@localhost:5432/QA"
db = create_engine(db_connection_string)

def test_db_connection():
    inspector = inspect(db)
    names = inspector.get_table_names()
    assert names[-2] == 'sales'

def test_select():
    connection = db.connect()     # Создаем соединение
    result = connection.execute(text("SELECT * FROM teacher"))
    rows = result.mappings().all()   # Получаем результат в виде словарей
    row1 = rows[0]

    assert row1['teacher_id'] == 33284


    connection.close()

def test_insert():
    connection = db.connect()
    transaction = connection.begin()

    sql = text("insert into teacher(\"email\") values (:new_email)")
    connection.execute(sql, {"new_email":"alex@gmail.com"})

    transaction.commit()
    connection.close()

def test_update():
    connection = db.connect()
    transaction = connection.begin()

    sql = text('UPDATE teacher SET email = :new_email where teacher_id = :id')
    connection.execute(sql, {"new_email": 'alex@gmail.com', 'id': None})

    transaction.commit()
    connection.close()

def test_delete():
    connection = db.connect()
    transaction = connection.begin()

    sql = text("DELETE FROM teacher WHERE teacher_id = :id")
    connection.execute(sql, {"id": None})

    transaction.commit()
    connection.close()