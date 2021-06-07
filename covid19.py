import pandas as pd 
import matplotlib.pyplot as plt 
import numpy as np 
import glob
import os

from pandas.core.frame import DataFrame



yourpath = '/home/aa1/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports_us/'

dict = {}
for root, dirs, files in os.walk(yourpath, topdown=False):
    for name in files:
        dict[name[:-4]]= pd.read_csv(yourpath + name)


twentyone={}
twenty = {}

for k,v in dict.items():
    if k[-1]=='1':
        twentyone[k]=v
    else:
        twenty[k]=v


df = pd.DataFrame()

for k,v in sorted(twenty.items()):
    df[k + ' confirmed'] = v['Confirmed']
    df[k + ' deaths'] = v['Deaths']

for k,v in sorted(twentyone.items()):
    df[k + ' confirmed'] = v['Confirmed']
    df[k + ' deaths'] = v['Deaths']