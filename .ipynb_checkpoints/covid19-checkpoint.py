from numpy.core.fromnumeric import shape
import pandas as pd 
import matplotlib.pyplot as plt 
import numpy as np 
import os
import requests  
import pyspark as ps


spark = (ps.sql.SparkSession.builder 
        .master("local[4]") 
        .appName("sparkSQL exercise") 
        .getOrCreate()
        )
sc = spark.sparkContext


yourpath = '/home/aa1/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports_us/'

dict = {}
for root, dirs, files in os.walk(yourpath, topdown=False):
    for name in files:
        dict[name[:-4]]= pd.read_csv(yourpath + name)

pop_density= pd.read_csv('/home/aa1/Covid-cause-and-effect/Population-Density_By_State.csv', delimiter=',')
pop_density.rename(columns={'GEO.display-label':'States'}, inplace=True)
pop_density.drop(columns=['GEO.id','GEO.id2'], axis=1, inplace=True)
pop_density.drop(index=[51],axis=0, inplace=True)

dc =pd.Series({'state_name':'District of Columbia','party':'democrat','name':'Muriel Bowser'})
governor= pd.read_csv('/home/aa1/Covid-cause-and-effect/us-governors.csv', delimiter=',')
pop_density.rename(columns={'state_name':'States'}, inplace=True)
governor.drop(columns=['state_name_slug','state_code','state_code_slug','votesmart','title','name_slug','first_name','middle_name','last_name','name_suffix','goes_by','pronunciation','gender','ethnicity','religion','openly_lgbtq','date_of_birth','entered_office','term_end','biography','phone','fax','latitude','longitude','address_complete','address_number','address_prefix','address_street','address_sec_unit_type','address_sec_unit_num','address_city','address_state','address_zipcode','address_type','website','contact_page','facebook_url','twitter_handle','twitter_url','photo_url'], axis=1, inplace=True)
poli_leader = governor.append(dc,ignore_index=True)
poli_leader = poli_leader.sort_values(by=['state_name'])


twentyone={}
twenty = {}

for k,v in dict.items():
    if k[-1]=='1':
        twentyone[k]=v
    else:
        twenty[k]=v


df = pd.DataFrame()
for k,v in sorted(twenty.items()):
    df['States']= v['Province_State']
    df['Lat']= v['Lat']
    df['Long']= v['Long_']
    break
df.drop(index=[8,12,13,42,55,56,57,58],axis=0, inplace=True)


new_df= df.merge(pop_density['Density per square mile of land area'], on=pop_density['States'], how='left')
new_df.drop(columns='key_0', inplace=True)
new_df1= new_df.merge(poli_leader['party'], on=pop_density['States'], how='left')
new_df1.drop(columns='key_0', inplace=True)



for k,v in sorted(twenty.items()):
    new_df1[k + ' confirmed'] = v['Confirmed']
    new_df1[k + ' deaths'] = v['Deaths']

for k,v in sorted(twentyone.items()):
    new_df1[k + ' confirmed'] = v['Confirmed']
    new_df1[k + ' deaths'] = v['Deaths']

print(new_df1)