import sqlite3


WEB_URL = "http://www.edu.ru/schools/catalog/school/{school_id}/"
KEYS = {
    "интернет сайт": "web_site",
    "e-mail": "email",
    "e-mail:": "email",
    "адрес": "address",
    "телефон": "phone",
    "тип": "school_type",
    "принадлежность": "affiliation",
    "директор": "director",
    "классы подготовки": "classes",
}


def db_conn():
    conn = sqlite3.connect('school.db')
    cursor = conn.cursor()
    return conn, cursor


def create_db():
    conn, cursor = db_conn()
    # Create table
    cursor.execute(
        '''CREATE TABLE schools
        (school_name text,
        web_site text,
        email text,
        address text,
        phone text,
        school_type text,
        affiliation text,
        director text,
        classes text,
        url text)'''
    )
    cursor.execute('''CREATE TABLE error404 (school_id text)''')
    conn.commit()


def view_db():
    conn, cursor = db_conn()
    cursor.execute('''SELECT school_name, 
        web_site, 
        email, 
        address,
        phone, 
        school_type,
        affiliation,
        director,
        classes,
        url FROM schools''')
 
    rows = cursor.fetchall()
    print(rows)


def _key(key):
    return KEYS[key.lower()]


if __name__ == '__main__':
    create_db()
    # view_db()
