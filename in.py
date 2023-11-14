import pandas as pd
import altair as alt
import panel as pn
#from turbodbc import connect, make_options, Megabytes
from os import path
import json
import io
import math
from pncharts1 import *
from sql import dmt,cld
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl
import param
#import win32com.client as win32
from pretty_html_table import build_table
#import os
from misc import options,css,fran,coun,em

cwd=""
today=datetime.today()
ACCENT='#FFCC33'
pn.extension(nthreads=8)
pn.extension('vega')
pn.extension('tabulator')
alt.data_transformers.disable_max_rows()


#temp = pn.template.MaterialTemplate(title='Automated Insights',accent_base_color=ACCENT,header_background=ACCENT,prevent_collision=True)
temp = pn.template.MaterialTemplate(title='Automated Insights',header_background=ACCENT)
#brush = alt.selection_point(fields=['CatalogNumber'],name='brush')
class A(param.Parameterized):
    df=param.DataFrame()
    cc = param.List()
    ch=''
    sdf=param.DataFrame(pd.DataFrame({'CatalogNumber':['']}))
    tdf=param.DataFrame()
    l2fc='L2 Stat Final Rev'
    l0fc='`Fcst Stat Final Rev'
    tt=param.String('')
    @pn.depends('tt',watch=True)
    def ct(self):
        return pn.pane.HTML(f"<h2><b>{a.tt}<b></h2>")
a=A()

unv=pn.widgets.TextInput(name="User Name")
psv=pn.widgets.PasswordInput(name="Password")
sab=pn.widgets.Button(name="Save",button_type='primary')

#expp = pn.Card(pn.Row(unv,psv,sab),title='Credentials',sizing_mode='stretch_width',style=dict(background='#FFFFFF'),header_background="#FFFFFF",header_color="#000000",collapsed=True).servable()
expp = pn.Card(pn.Row(unv,psv,sab),title='Credentials',sizing_mode='stretch_width',header_background="#FFFFFF",header_color="#000000",collapsed=True).servable()

try:
    with open('config.json') as json_file:
        data = json.load(json_file)
        un = data.get('username')
        pp = data.get('password')
        unv.value=un
        psv.value=pp
except:
    pass

def sabf(e):
    pp={'username':unv.value,'password':psv.value}
    with open('config.json', 'w+') as outfile:
        outfile.write(json.dumps(pp))
sab.on_click(sabf)

fcs=pn.widgets.Select(name="Forecast Type",options=['Stat Fcst','DF Fcst'],height=40,width=130)
co=pn.widgets.Select(name="Select Country",options=coun,value='INDIA',height=40)
fr=pn.widgets.Select(name='Select Franchise',options=fran,value='Joint Replacement',height=40)
def ftf(e):
    print(e)
    if e.new=='Stat Fcst':
        a.l2fc='L2 Stat Final Rev'
        a.l0fc='`Fcst Stat Final Rev'
    else:
        a.l2fc='L2 DF Final Rev'
        a.l0fc='`Fcst DF Final Rev'
fcs.param.watch(ftf,'value')

enb=pn.widgets.Button(name="Envision",button_type='primary',sizing_mode='stretch_height',height=40, align='end',margin=(12,10))
ldb=pn.widgets.Button(name="Load",button_type='primary',sizing_mode='stretch_height',height=40,margin=(12,10))
w3=pn.Row(co,fr,enb,ldb,height=50).servable()

sk=pn.widgets.EditableIntSlider(name="SKU Rank",start=50,end=800, step=10, value=100)
w4=pn.Row(fcs,sk,margin=(18,5)).servable()
conb=pn.widgets.Button(name="Contribution",button_style='outline',button_type='warning',sizing_mode='stretch_height')
accb=pn.widgets.Button(name="Accuracy",button_style='outline',sizing_mode='stretch_height',button_type='warning')
covb=pn.widgets.Button(name="COV",button_style='outline',sizing_mode='stretch_height',button_type='warning')
corb=pn.widgets.Button(name="Correlation",button_style='outline',sizing_mode='stretch_height',button_type='warning')
fccb=pn.widgets.Button(name="Change",button_style='outline',sizing_mode='stretch_height',button_type='warning')
lttb=pn.widgets.Button(name="Long Term",button_style='outline',sizing_mode='stretch_height',button_type='warning')

t1=pn.Row(conb,accb,covb,corb,fccb,lttb,height=50).servable()

def data(e):
    ss="gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net"
    #cnxn.close()
    print(fr.value,co.value)
    #cnxn=connect(DRIVER='ODBC Driver 17 for SQL Server',server=ss,user=f'{unv}',password=psv,database="gda_glbsyndb",Trusted_Connection='yes', turbodbc_options=options)
    if fr.value in ['Instruments','Joint Replacement','Spine','Trauma and Extremities','Neurovascular']:
        query = dmt()
    elif fr.value in ['CMF','Endoscopy','Sustainability']:
        query = cld()
    cur=cnxn.cursor()
    print('PULLING DATA!!')
    cur.execute(query,[fr.value,'','','','','', co.value])
    dd=cur.fetchallarrow()
    df=pl.DataFrame(dd)
    df.write_parquet(f'{[co.value]}-{[fr.value]}.parquet')
    print('DONE!!')
    cnxn.close()
    cur.close()

cp=pn.Row()

def lpr(e):
    #df=pd.read_parquet(f'{cwd}\\{[co.value]}-{[fr.value]}.parquet') # CHANGE TO POLARS
    df=pd.DataFrame()
    df=pl.from_pandas(df)
    #df1=df[(df['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (df['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12))] # CHANGE TO POLARS
    #df1.loc[df1['SALES_DATE']<datetime(today.year,today.month,1),'actwfc']= df1['`Act Orders Rev'].copy() # CHANGE TO POLARS
    #df1.loc[df1['SALES_DATE']>=datetime(today.year,today.month,1),'actwfc']= df1['`Fcst DF Final Rev'].copy() # CHANGE TO POLARS
    #cc=df1.groupby('CatalogNumber').sum(numeric_only=True)[['actwfc']].sort_values(by='actwfc',ascending=False).reset_index()['CatalogNumber'][:sk.value].values.tolist() # CHANGE TO POLARS
    #a.df=df.copy()
    #df=pl.read_parquet(f"C:\\Users\\smishra14\\OneDrive - Stryker\\python\\app\\['INDIA']-['Joint Replacement'].parquet")
    df1=df.filter((df['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (df['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)))
    df1=df1.with_columns((pl.when(df1['SALES_DATE']<datetime(today.year,today.month,1)).then(df1['`Act Orders Rev']).otherwise(df1['`Fcst DF Final Rev'])).alias('actwfc'))
    cc=list(df1.group_by('CatalogNumber').sum().sort(by='actwfc',descending=True)['CatalogNumber'][:sk.value])
    a.cc=cc
    #df2=pl.from_pandas(df)
    df1=df.filter(df["CatalogNumber"].is_in(cc))
    df1=df1.with_columns(abs(df1['`Act Orders Rev']-df1[a.l2fc]).alias('L2 Abs Var'))
    df1=df1.with_columns((1-df1['L2 Abs Var']/df1['`Act Orders Rev']).alias('L2 Acc'))
    df1=df1.with_columns(pl.when(df1['`Act Orders Rev']==0).then(1).otherwise(df1['L2 Acc']))
    df1=df1.with_columns(df1['L2 Acc'].clip(0,df1['L2 Acc'].max()).alias('L2 Acc'))
    df1=df1.filter((df1['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=24)) & (df1['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=24)))
    acc1=df1.filter((df1['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=3)) & (df1['SALES_DATE']<=datetime(today.year,today.month,1)-relativedelta(months=1)))
    acc1=acc1.with_columns((acc1['`Act Orders Rev']/acc1['`Act Orders Rev'].sum()*100).alias('orders cont'))
    acc1=acc1.with_columns((acc1['L2 Abs Var']/acc1['L2 Abs Var'].sum()*100).alias('var cont'))
    acc1=acc1[['IBP Level 5','CatalogNumber','SALES_DATE','orders cont','var cont','L2 Acc']]
    df1=df1.join(acc1,on=['CatalogNumber','SALES_DATE'],how='left')
    df1=df1[['IBP Level 5','CatalogNumber','SALES_DATE','`Act Orders Rev',a.l0fc,a.l2fc,'orders cont','var cont','L2 Acc']]
    df1=df1.with_columns((df1[a.l2fc]-df1[a.l0fc]).alias('change'))
    df1=df1.with_columns(abs(df1['change']).alias('change'))
    df1=df1.melt(['IBP Level 5','CatalogNumber','SALES_DATE','orders cont','var cont','L2 Acc','change'])
    df1=df1.rename({'variable':'type'})
    df1=df1.with_columns(df1['L2 Acc'].diff().alias('Decrease'))
    #df1=df1.filter(df1['Decrease']<0).sort('Decrease')
    a.df=df1.to_pandas()
    #print(a.df)

async def conp(e):
    conb.button_style,accb.button_style,covb.button_style,corb.button_style,fccb.button_style,lttb.button_style='outline','outline','outline','outline','outline','outline'
    conb.button_style='solid'
    print(a.df)
    a.ch=cont(a.df,a.l2fc,a.l0fc,a.cc,cp)
    cp.clear()
    cp.append(a.ch)
    a.tt='Contribution'
    rdf=pd.DataFrame(a.ch.selection.brush)  # Not working
    #rdf['Reason']='Contribution'
    print(rdf)
    a.sdf=pd.concat([a.sdf,rdf])
    print(a.sdf)
async def accp(e):
    conb.button_style,accb.button_style,covb.button_style,corb.button_style,fccb.button_style,lttb.button_style='outline','outline','outline','outline','outline','outline'
    accb.button_style='solid'
    a.ch=acct(a.df,a.l2fc,a.cc,sk.value,cp)
    cp.clear()
    cp.append(a.ch)
    rdf=pd.DataFrame(a.ch.selection.brush)  # Not working
    #rdf['Reason']='Accuracy'
    a.tt='Accuracy'
    a.sdf=pd.concat([a.sdf,rdf])
async def corp(e):
    conb.button_style,accb.button_style,covb.button_style,corb.button_style,fccb.button_style,lttb.button_style='outline','outline','outline','outline','outline','outline'
    corb.button_style='solid'
    cp.clear()
    a.ch=cort(a.df,a.l0fc,'stat',a.cc,cp)
    cp.append(a.ch)
    rdf=pd.DataFrame(a.ch.selection.brush)  # Not working
    #rdf['Reason']='Correlation'
    a.tt='Correlation'
    a.sdf=pd.concat([a.sdf,rdf])
async def chap(e):
    conb.button_style,accb.button_style,covb.button_style,corb.button_style,fccb.button_style,lttb.button_style='outline','outline','outline','outline','outline','outline'
    fccb.button_style='solid'
    cp.clear()
    a.ch=fct(a.df,a.l2fc,a.l0fc,cp)
    cp.append(a.ch)
    rdf=pd.DataFrame(a.ch.selection.brush)   # Not working
    #rdf['Reason']='Change'
    a.tt='Change'
    a.sdf=pd.concat([a.sdf,rdf])
async def lonp(e):
    conb.button_style,accb.button_style,covb.button_style,corb.button_style,fccb.button_style,lttb.button_style='outline','outline','outline','outline','outline','outline'
    lttb.button_style='solid'
    a.ch=ltt(a.df,cp)
    cp.clear()
    cp.append(a.ch)
    #rdf=pd.DataFrame(a.ch.selection.brush)  # Not working
    #rdf['Reason']='Contribution'
    a.tt='Long Term'
    #a.sdf=pd.concat([a.sdf,rdf])
async def covp(e):
    conb.button_style,accb.button_style,covb.button_style,corb.button_style,fccb.button_style,lttb.button_style='outline','outline','outline','outline','outline','outline'
    covb.button_style='solid'
    a.ch=covt(a.df,a.l2fc,a.l0fc,a.cc,cp)
    cp.clear()
    cp.append(a.ch)
    rdf=pd.DataFrame(a.ch.selection.brush)  # Not working
    #rdf['Reason']='COV'
    a.tt='COV'
    a.sdf=pd.concat([a.sdf,rdf])

conb.on_click(conp)
accb.on_click(accp)
covb.on_click(covp)
corb.on_click(corp)
fccb.on_click(chap)
lttb.on_click(lonp)

def dtpf(e):
    a.tdf=pd.concat([a.tdf,dt()],ignore_index=True)
    tb.clear()
    tb.append(pn.widgets.Tabulator(a.tdf))
def tt(e):
    print(a.sdf)
dnb=pn.widgets.Button(name="Download",button_type='primary',margin=(15,15)).servable()
dtpb=pn.widgets.Button(name="Add Exception",button_type='primary',margin=(15,15)).servable()
dnb.on_click(tt)
dtpb.on_click(dtpf)

def enbf(e):
    #df=data(co.value,fr.value,sk.value)
    df=data()
    df.to_parquet(f'{co.value}-{fr.value}.parquet')



def exp(e):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        df.to_excel(writer, sheet_name='Sheet1',index=False)
        writer.close()
    return buffer

enb.on_click(data)
ldb.on_click(lpr)

def smf(e):  ##Send MAIL
    pass

mi=pn.widgets.AutocompleteInput(name="Email ID",options=em, restrict=False)
smb=pn.widgets.Button(name="Send",button_type='primary',on_click=smf,margin=(15,9))
smc= pn.Card(pn.Row(mi,smb),title='Send Mail',collapsed=True,styles={'width':'440px'}).servable()
mr=pn.Row(dnb,smc,dtpb,a.ct)

cp.append(a.ch)
tb=pn.Row(pn.widgets.Tabulator(a.tdf))

temp.main.append(expp)
temp.main.append(w3)
temp.main.append(w4)
temp.main.append(t1)
temp.main.append(mr)
temp.main.append(cp)
temp.main.append(tb)

pn.config.raw_css.append(css)
#temp.config.raw_css.append(css)
#pn.serve(temp)  # USE WITH PYTHON IN.PY
temp.show(websocket_origin='forecastreview.onrender.com')  # USE WITH PANEL SERVE
