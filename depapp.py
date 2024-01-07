import pandas as pd
import altair as alt
import panel as pn
import json
import io
from pncharts1 import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl
import param
from misc import options,css,fran,coun,em

cwd=""
today=datetime.today()
ACCENT='#FFCC33'
pn.extension(nthreads=8)
pn.extension('vega')
pn.extension('tabulator')
alt.data_transformers.disable_max_rows()

temp = pn.template.MaterialTemplate(title='Automated Insights',header_background=ACCENT)


unv=pn.widgets.TextInput(name="User Name")
psv=pn.widgets.PasswordInput(name="Password")
sab=pn.widgets.Button(name="Save",button_type='primary')
fcs=pn.widgets.Select(name="Forecast Type",options=['Stat Fcst','DF Fcst'],height=40,width=130)
co=pn.widgets.Select(name="Select Country",options=coun,value='INDIA',height=40)
fr=pn.widgets.Select(name='Select Franchise',options=fran,value='Joint Replacement',height=40)

w3=pn.Row(unv,psv,sab,fcs,height=50).servable()
temp.main.append(w3)


temp.show(websocket_origin='forecastreview.onrender.com')  # USE WITH PANEL SERVE
