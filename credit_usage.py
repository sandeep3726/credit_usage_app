import pandas as pd
import snowflake.connector
import numpy as np
import time
import streamlit as st
# from matplotlib import pyplot as plt
# import plotly.express as px




print("Connecting....")
connector = snowflake.connector.connect(
    account = 'odgkiqc-sa05981',
    user = 'REENA0856',
    password = 'S@nju12345',
    warehouse = 'COMPUTE_WH',
    role = 'ACCOUNTADMIN',
    database = 'FIRST_DATABASE',
    schema = 'PUBLIC'
)

#for wide page
st. set_page_config(layout="wide")

#calling the snowflake connection function
# def snowflake_conn():    
#     return snowflake.connector.connect(**st.secrets["snowflake"])
# conn = snowflake_conn()

sql1 = '''select account_name||'- Daily Usage' Account,to_char(usage_date) as Date,round(sum(usage_in_currency),3) Usage_in_Usd
from snowflake.organization_usage.usage_in_currency_daily 
where usage_date = (select max(usage_date) 
from snowflake.organization_usage.usage_in_currency_daily)  
group by account_name,usage_date 

union all

select account_name||'-Monthly Usage' Account,to_char(monthname(current_date()) ) as Date,round(sum(usage_in_currency),3) Usage_in_Usd 
from snowflake.organization_usage.usage_in_currency_daily 
where usage_date > DATE_TRUNC('month', current_date()) 
group by account_name,month(current_date()) 
order by Account desc;
  ;'''

sql2 = '''select day(usage_date) as DAY, round(sum(usage_in_currency),2) as CREDIT_CONSUMPTION
FROM "SNOWFLAKE"."ORGANIZATION_USAGE"."USAGE_IN_CURRENCY_DAILY"
group by 1
order by 1 ;'''

df1 = pd.read_sql(sql1,con=connector)
df2 = pd.read_sql(sql2,con=connector)

st.table(df1)
st.bar_chart(data = df2, x= "DAY", y = "CREDIT_CONSUMPTION")

st.subheader("credit consumption")
fig = px.bar(data_frame = df2, x="day", y="credit_consumption",template = "simple_white")
fig.update_layout()        
st.plotly_chart(fig,use_container_width=True)
