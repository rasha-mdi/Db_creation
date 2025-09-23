import psycopg2
import numpy as np
import pandas as pd

hostname = 'localhost'
database = 'electricity2'
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
    kw_1 float NOT NULL,
    kw_2 float NOT NULL,
    kw_3 float NOT NULL,
    kw_4 float NOT NULL,
    kw_5 float NOT NULL,
    kw_6 float NOT NULL,
    kw_7 float NOT NULL,
    kw_8 float NOT NULL,
    kw_9 float NOT NULL,
    kw_10 float NOT NULL,
    kw_11 float NOT NULL,
    kw_12 float NOT NULL,
    kw_13 float NOT NULL,
    kw_14 float NOT NULL,
    kw_15 float NOT NULL,
    kw_16 float NOT NULL,
    kw_17 float NOT NULL,
    kw_18 float NOT NULL,
    kw_19 float NOT NULL,
    kw_20 float NOT NULL,
    kw_21 float NOT NULL,
    kw_22 float NOT NULL,
    kw_23 float NOT NULL,
    kw_24 float NOT NULL,
    kw_25 float NOT NULL,
    kw_26 float NOT NULL,
    kw_27 float NOT NULL,
    kw_28 float NOT NULL,
    kw_29 float NOT NULL,
    kw_30 float NOT NULL,
    kw_31 float NOT NULL,
    kw_32 float NOT NULL,
    kw_33 float NOT NULL,
    kw_34 float NOT NULL,
    kw_35 float NOT NULL,
    kw_36 float NOT NULL,
    kw_37 float NOT NULL,
    kw_38 float NOT NULL,
    kw_39 float NOT NULL,
    kw_40 float NOT NULL,
    kw_41 float NOT NULL,
    kw_42 float NOT NULL,
    kw_43 float NOT NULL,
    kw_44 float NOT NULL,
    kw_45 float NOT NULL,
    kw_46 float NOT NULL,
    kw_47 float NOT NULL,
    kw_48 float NOT NULL,
    PRIMARY KEY (date, m_id)
    )
    '''
    cur.execute(create_script)

    file_path = r'C:\Users\rasha\Downloads\speedy.csv'
    df= pd.read_csv(file_path)
    i=0
    for index,row in df.iterrows():

        i=i+1
        insert_script =f"INSERT INTO meters (m_id, MPAN) VALUES ('{i}', '{row['MPAN']}')"
        #for a in range (1,49):
            #value = row[f'kWh_{a}']
        insert_script2 =f"INSERT INTO consumption (date, m_id, kw_1, kw_2, kw_3, kw_4, kw_5, kw_6, kw_7, kw_8, kw_9, kw_10, kw_11, kw_12, kw_13, kw_14, kw_15, kw_16, kw_17, kw_18, kw_19, kw_20,kw_21, kw_22, kw_23, kw_24, kw_25, kw_26, kw_27, kw_28, kw_29, kw_30,kw_31, kw_32, kw_33, kw_34, kw_35, kw_36, kw_37, kw_38, kw_39, kw_40,kw_41, kw_42, kw_43, kw_44, kw_45, kw_46, kw_47, kw_48 ) VALUES ('{row['ConsumptionDate']}','{i}','{row['kWh_1']}', '{row['kWh_2']}', '{row['kWh_3']}', '{row['kWh_4']}', '{row['kWh_5']}', '{row['kWh_6']}', '{row['kWh_7']}', '{row['kWh_8']}', '{row['kWh_9']}', '{row['kWh_10']}','{row['kWh_11']}', '{row['kWh_12']}', '{row['kWh_13']}', '{row['kWh_14']}', '{row['kWh_15']}', '{row['kWh_16']}', '{row['kWh_17']}', '{row['kWh_18']}', '{row['kWh_19']}', '{row['kWh_20']}','{row['kWh_21']}', '{row['kWh_22']}', '{row['kWh_23']}', '{row['kWh_24']}', '{row['kWh_25']}', '{row['kWh_26']}', '{row['kWh_27']}', '{row['kWh_28']}', '{row['kWh_29']}', '{row['kWh_30']}','{row['kWh_31']}', '{row['kWh_32']}', '{row['kWh_33']}', '{row['kWh_34']}', '{row['kWh_35']}', '{row['kWh_36']}', '{row['kWh_37']}', '{row['kWh_38']}', '{row['kWh_39']}', '{row['kWh_40']}', '{row['kWh_41']}', '{row['kWh_42']}', '{row['kWh_43']}', '{row['kWh_44']}', '{row['kWh_45']}', '{row['kWh_46']}', '{row['kWh_47']}', '{row['kWh_48']}')"
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