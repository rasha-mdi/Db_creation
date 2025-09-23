import psycopg2
import numpy as np
import pandas as pd
import json

hostname = 'localhost'
database = 'electricity'
username='postgres' 
pwd='Irfan@5011'
port_id=5432

conn=None
cur=None
try:
    conn=psycopg2.connect(
        host=hostname,

        dbname=database,
        user=username,
        password=pwd,
        port=port_id)
    cur=conn.cursor()

    cur.execute("DROP TABLE IF EXISTS consumption")
    cur.execute('DROP TABLE IF EXISTS meters')
    create_script='''CREATE TABLE IF NOT EXISTS meters(
    m_id int PRIMARY KEY,
    MPAN varchar(256) NOT NULL
    );
    CREATE TABLE IF NOT EXISTS consumption(
    date date,
    m_id int NOT NULL,
    FOREIGN KEY (m_id) REFERENCES meters(m_id),
    consumed jsonb NOT NULL,
    PRIMARY KEY (date, m_id)
    )
    '''
    cur.execute(create_script)

    file_path = r'C:\Users\rasha\Downloads\speedy.csv'
    df= pd.read_csv(file_path)
    i=0
    for index,row in df.iterrows():

        jsr=df.iloc[index,3:]
        rj=json.dumps(dict(jsr))

        i=i+1
        insert_script =f"INSERT INTO meters (m_id, MPAN) VALUES ('{i}', '{row['MPAN']}')"

        insert_script2 =f"INSERT INTO consumption (date, m_id, consumed) VALUES ('{row['ConsumptionDate']}','{i}','{rj}')"
        #insert_value= row['MPAN']
        #for record in insert_value:
        cur.execute(insert_script)
        cur.execute(insert_script2)

    conn.commit()
    
except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:   
        conn.close()