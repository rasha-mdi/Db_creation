import psycopg2
import pandas as pd
import json
import re

hostname = 'localhost'
database = 'electricity'
username = 'postgres'
pwd = 'Irfan@5011'
port_id = 5432

conn = None
cur = None
try:
    conn = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    cur = conn.cursor()

    cur.execute("SELECT * FROM consumption;")
    data = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(data, columns=columns)


    expanded = pd.json_normalize(df["consumed"])
    df = df.join(expanded)


    df['date'] = pd.to_datetime(df['date'])

    df['total'] = (df['00:00'] + df['00:30'] + df['01:00'] + df['01:30'] +df['02:00'] + df['02:30'] + df['03:00'] + df['03:30'] +df['04:00'] + df['04:30'] + df['05:00'] + df['05:30'] +df['06:00'] + df['06:30'] + df['07:00'] + df['07:30'] +df['08:00'] + df['08:30'] + df['09:00'] + df['09:30'] +df['10:00'] + df['10:30'] + df['11:00'] + df['11:30'] +df['12:00'] + df['12:30'] + df['13:00'] + df['13:30'] +df['14:00'] + df['14:30'] + df['15:00'] + df['15:30'] +df['16:00'] + df['16:30'] + df['17:00'] + df['17:30'] +df['18:00'] + df['18:30'] + df['19:00'] + df['19:30'] +df['20:00'] + df['20:30'] + df['21:00'] + df['21:30'] +df['22:00'] + df['22:30'] + df['23:00'] + df['23:30'])


# 2.1 At what time of the day (hour or half-hour slot) does each meter record the highest consumption?
    
    week1 = df.iloc[32:67,:-1] 
    time_cols = df.columns[3:-1].tolist()

    slot_totals = week1.groupby('m_id')[time_cols].sum().reset_index()

    result = []
    for _, row in slot_totals.iterrows():
        m_id = row['m_id']
        max_val = row[time_cols].max()
        max_slots = [col for col in time_cols if row[col] == max_val]
        for slot in max_slots:
            result.append([m_id, slot, max_val])
    
    #print(pd.DataFrame(result, columns=['m_id', 'time_slot', 'consumption']))

# 2.2 How does consumption differ between weekdays and weekends for each meter?

    '''week= df.copy()
    week['dayno.'] = week['date'].dt.dayofweek
    week['day'] = week['dayno.'].apply(lambda x: 'Weekday' if x <= 4 else 'Weekend')

    print(week.groupby(['m_id', 'day'])['total'].sum().reset_index())'''

    week1 = df.iloc[32:67].copy()
    week1['dayno.'] = week1['date'].dt.dayofweek
    week1['day'] = week1['dayno.'].apply(lambda x: 'Weekday' if x <= 4 else 'Weekend')

    #print(week1.groupby(['m_id', 'day'])['total'].sum().reset_index())

# 2.3 How does the total consumption of each meter compare (take any one available month)?

    months=df.copy()
    months['year'] = months['date'].dt.year
    months['month'] = months['date'].dt.month

    months2 = months[((months['month'] == 12) & (months['year'] == 2023))|((months['month'] == 1) & (months['year'] == 2024))]
    
    #print(months2.groupby(['m_id', 'year', 'month'])['total'].sum().reset_index())

    '''for _, row in df.iterrows():
        month = df['total'].dt.month
        year = df['total'].dt.year
        if (month == 12 and year == 2023) or (month == 1 and year == 2024):
            print(row)'''
    
    #totalbymetermonth= df.groupby('m_id')['total'].dt.month

# 2.4 Which are the top 5 meters with the highest total consumption and the bottom 5 meters with the lowest total consumption?

    totalbymeter = df.groupby('m_id')['total'].sum().reset_index()

    #print(totalbymeter.sort_values('total', ascending=False).head(5))

    #print(totalbymeter.sort_values('total', ascending=True).head(5))

# 2.5 What are the 7-day rolling average consumption values for each meter, and on which days does consumption exceed 2× the rolling average (spike detection)?
    months1=  months[(months['month'] == 12) & (months['year'] == 2023)]

    months1.sort_values(by=['m_id', 'date'], inplace=True)

    months1['rolling']=df['total'].rolling(window=7).mean()

    months1['spikedetect'] = (months1['total']) > (2*months1['rolling'])
    #print(months1[months1['spikedetect'] == True][['m_id', 'date', 'total', 'rolling', 'spikedetect']])

# 2.6 On which days did each meter record the lowest consumption, and what were those values?

    mintotal = df.groupby('m_id')['total'].min().reset_index()

    #print(pd.merge(df[['m_id', 'date', 'total']], mintotal, on=['m_id', 'total']))

# 2.7 Which meters have similar usage patterns (find correlation between meters (corr()) for a single day)

    day1= df[df['date'] == '2024-01-30'].copy()
    time_cols= df.columns[3:-1].tolist()

    slottotals = day1.groupby('m_id')[time_cols].sum().reset_index()

    meter_ids = slottotals['m_id'].tolist()
    for i in range(len(meter_ids)):
        for j in range(i+1, len(meter_ids)):
            meteri = slottotals.iloc[i][time_cols]
            meterj = slottotals.iloc[j][time_cols]
        
            corr_coef = meteri.corr(meterj)
            print(f"meters {meter_ids[i]} and {meter_ids[j]}: {corr_coef}")

# 2.8 On which days does each meter show abnormally high or low consumption compared to its average (anomaly detection)?



# 2.9 What is the load factor (average load ÷ peak load × 100) of each meter, and which meters have the lowest load factor (poor utilization)?



except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
