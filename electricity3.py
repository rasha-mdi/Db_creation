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

# 1.1. extract data from db and convert in to db

    cur.execute("SELECT * FROM consumption;")
    data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(data, columns=columns)

    #print(df.head(5))

# 1.2. Basic Exploration (View & Inspect Data ,Check Missing Values,Filtering )
    
    expanded = pd.json_normalize(df["consumed"])
    df = df.join(expanded)
    #print(df.loc[df['00:30']>5])
    #print(df.head(5))
    
    # missing values
    missing_values= df.isnull().sum()
    #print(missing_values)

    df['date']=pd.to_datetime(df['date'])
    #print(df['date'].dtype)

    #print(df.loc[(df['m_id'] == 1) & (df['date'].dt.year == 2025)])

# 1.3. Total consumption  of  each meter

    #print(df.groupby(['m_id']).sum())
    df['total'] = (df['00:00'] + df['00:30'] + df['01:00'] + df['01:30'] +df['02:00'] + df['02:30'] + df['03:00'] + df['03:30'] +df['04:00'] + df['04:30'] + df['05:00'] + df['05:30'] +df['06:00'] + df['06:30'] + df['07:00'] + df['07:30'] +df['08:00'] + df['08:30'] + df['09:00'] + df['09:30'] +df['10:00'] + df['10:30'] + df['11:00'] + df['11:30'] +df['12:00'] + df['12:30'] + df['13:00'] + df['13:30'] +df['14:00'] + df['14:30'] + df['15:00'] + df['15:30'] +df['16:00'] + df['16:30'] + df['17:00'] + df['17:30'] +df['18:00'] + df['18:30'] + df['19:00'] + df['19:30'] +df['20:00'] + df['20:30'] + df['21:00'] + df['21:30'] +df['22:00'] + df['22:30'] + df['23:00'] + df['23:30'])
    #print(df.groupby('m_id')[['total']].sum())

# 1.4. On which day of the week is consumption very high? (for every m_id)

    df['day'] = df['date'].dt.dayofweek

    maxday=df.groupby('m_id')['total'].max()

    #print(pd.merge(df[['m_id', 'day', 'total']], maxday, on=['m_id', 'total']))


# 1.5. What is the monthly consumption  each meter ?  (Take 2024 data)

    df['month']=df['date'].dt.month

    y2024 = df[df['date'].dt.year == 2024]
    #print (y2024.groupby(['m_id', 'month'])['total'].sum())

# 1.6 .Take particular year from the data, and then find for each meter which month energy consumption is very high

    y2023 = df[df['date'].dt.year == 2023]

    monthlyc23 = y2023.groupby(['m_id', 'month'])['total'].sum().reset_index()

    maxmc23 = monthlyc23.groupby('m_id')['total'].max()

    #print(pd.merge(monthlyc23, maxmc23, on=['m_id', 'total']))

# 2.1 At what time of the day (hour or half-hour slot) does each meter record the highest consumption?

    #week1 = df.groupby(['day'])
    #print(df.iloc[6:13,3:])
    '''week1=df.iloc[32:67]
    time_cols = df.columns[3:] 
    week_long = week1.melt(id_vars=['m_id', 'date'], value_vars=time_cols, var_name='time_slot', value_name='consumption')
    slot_totals = week_long.groupby(['m_id', 'time_slot'])['consumption'].sum().reset_index()
    max_slots = slot_totals.groupby('m_id', as_index=False)['consumption'].max()
    print(pd.merge(slot_totals, max_slots, on=['m_id', 'consumption']))'''

    week1 = df.iloc[32:67]

    time_cols = df.columns[3:-2]

    slot_totals = week1.groupby('m_id')[time_cols].sum().reset_index()

    max_values = slot_totals.set_index('m_id').max(axis=1).reset_index()
    max_values.columns = ['m_id', 'consumption']

    result = []
    for _, row in slot_totals.iterrows():
        m_id = row['m_id']
        max_val = max_values.loc[max_values['m_id'] == m_id, 'consumption'].values[0]
        max_slots = [col for col in time_cols if row[col] == max_val]
        for slot in max_slots:
            result.append([m_id, slot, max_val])

    print(pd.DataFrame(result, columns=['m_id', 'time_slot', 'consumption']))





except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:   
        conn.close()