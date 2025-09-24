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
#from
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
    df.columns=['siteRef','MPAN','ConsumptionDate','00:00', '00:30','01:00', '01:30','02:00', '02:30','03:00', '03:30','04:00', '04:30','05:00', '05:30','06:00', '06:30','07:00', '07:30','08:00', '08:30','09:00', '09:30','10:00', '10:30','11:00', '11:30','12:00', '12:30','13:00', '13:30','14:00', '14:30','15:00', '15:30','16:00', '16:30','17:00', '17:30','18:00', '18:30','19:00', '19:30','20:00', '20:30','21:00', '21:30','22:00', '22:30','23:00', '23:30']
    
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
#to  
except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:   
        conn.close()