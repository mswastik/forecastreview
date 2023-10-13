from sanic import Sanic
from reactpy import component, html, run, use_callback,use_state,web,utils
from reactpy.backend.sanic import configure,Options
from turbodbc import connect, make_options, Megabytes
import json
import io
import pandas as pd
import altair as alt
import uvicorn
#import vegafusion as vf
#from bokeh.plotting import figure
#from bokeh.resources import CDN
#from bokeh.embed import file_html,json_item
#from bokeh.layouts import gridplot
from sql import dmt,cld
#import connectorx as cx
import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
#from dtale import show


#alt.data_transformers.disable_max_rows()
alt.data_transformers.enable("vegafusion")
headv=html._(html.link({"rel":"stylesheet","href":"https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.5.0/semantic.min.css"}),
                html.script({'src':"https://code.jquery.com/jquery-3.1.1.min.js",'integrity':"sha256-hVVnYaiADRTO2PzUGmuLJr8BLUSjGIZsDYGmIJLv2b8=",'crossorigin':"anonymous"}),
                html.script({"src":"https://cdnjs.cloudflare.com/ajax/libs/semantic-ui/2.5.0/semantic.min.js","integrity":"sha512-Xo0Jh8MsOn72LGV8kU5LsclG7SUzJsWGhXbWcYs2MAmChkQzwiW/yTQwdJ8w6UA9C6EVG18GHb/TrYpYCjyAQw==","crossorigin":"anonymous", "referrerpolicy":"no-referrer"}),
                html.script({"src":"https://cdn.jsdelivr.net/npm/vega@5"}),
                html.script({"src":"https://cdn.jsdelivr.net/npm/vega-lite@5"}),
                html.script({"src":"https://cdn.jsdelivr.net/npm/vega-embed@6"}),
                #html.script({"language":"javascript","src":"https://cdnjs.cloudflare.com/ajax/libs/bokeh/3.2.0/bokeh.min.js"}),
                )

options = make_options(read_buffer_size=Megabytes(300),
                        parameter_sets_to_buffer=1000,
                        varchar_max_character_limit=1000,
                        use_async_io=True,
                        prefer_unicode=True,
                        large_decimals_as_64_bit_types=True,
                        limit_varchar_results_to_max=True)

catcol=['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']
divcol=['rgb(0,0,255)', 'rgb(51,153,255)', 'rgb(102,204,255)', 'rgb(153,204,255)', 'rgb(204,204,255)', 'rgb(255,255,255)', 'rgb(255,204,255)', 'rgb(255,153,255)', 'rgb(255,102,204)', 'rgb(255,102,102)', 'rgb(255,0,0)']
ordcol= ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52']

def alt_theme():
    return {
        'config': {
            'view':{
                'stroke':'transparent'
            },
            'title': {
                'titleColor':'#616161',
                'color':'#424242'
            },
            'axis': {
                'gridColor':'#EEEEEE',
                'titleFontSize':14,
                'labelFontSize':13,
                'titleFontStyle':500
            },
            "range": {
                "category": ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52"],
                "diverging": ["rgb(0,0,255)", "rgb(51,153,255)", "rgb(102,204,255)", "rgb(153,204,255)", "rgb(204,204,255)", "rgb(255,255,255)", "rgb(255,204,255)", "rgb(255,153,255)", "rgb(255,102,204)", "rgb(255,102,102)", "rgb(255,0,0)"],
                "heatmap": ['rgb(0,0,255)', 'rgb(51,153,255)', 'rgb(102,204,255)', 'rgb(153,204,255)', 'rgb(204,204,255)', 'rgb(255,255,255)', 'rgb(255,204,255)', 'rgb(255,153,255)', 'rgb(255,102,204)', 'rgb(255,102,102)', 'rgb(255,0,0)'],
                "ramp": ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'],
                "ordinal": ['#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'],
             },
               }
    }

alt.themes.register("alt_theme", alt_theme)
alt.themes.enable("alt_theme")

fran=['CMF','Instruments','Joint Replacement', 'Trauma and Extremities','Endoscopy','Spine']
coun=['INDIA', 'CHINA','UNITED STATES','JAPAN']

def data(co,fr,uni,pwi):
    ss="gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net"
    #uni = str(uni).replace("@","%40")+"@gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net"
    #pwi = str(pwi).replace("#","%23")
    #ss=f"mssql://{uni}:{pwi}@gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net:1433/gda_glbsyndb?encrypt=true"
    cnxn=connect(DRIVER='ODBC Driver 17 for SQL Server',server=ss,user=f'{uni}',password=pwi,database="gda_glbsyndb",Trusted_Connection='yes', turbodbc_options=options)
    #cnxn=connect(DRIVER='ODBC Driver 17 for SQL Server',server=ss,user=f'{uni}@{ss}',password=pwi,database="gda_glbsyndb", turbodbc_options=options)
    if fr[0] in ['Instruments','Joint Replacement','Spine','Trauma and Extremities','Neurovascular']:
        query = dmt()
    elif fr[0] in ['CMF','Endoscopy','Sustainability']:
        query = cld()
    cur=cnxn.cursor()
    #df=cx.read_sql(ss, query, partition_num=10)
    print('PULLING DATA!!')
    print([fr[i] for i in range(len(fr)) if fr[i]]+ ['' for i in range(len(fran)-len(fr))] + [co])
    cur.execute(query,[fr[i] for i in range(len(fr)) if fr[i]]+ ['' for i in range(len(fran)-len(fr))] + [co])
    #dd=cur.fetchallnumpy()
    dd=cur.fetchallarrow()
    print('DONE!!')
    df=pl.DataFrame(dd)
    cnxn.close()
    cur.close()
    return df

def cont(df,l2fc,l0fc,fci,cc):  #POLARS
    if not df.is_empty():
        today=datetime.today()
        acc=df.clone()
        acc=acc.filter(acc['CatalogNumber'].is_in(cc))
        acc=acc.with_columns(abs(acc['`Act Orders Rev']-acc[l2fc]).alias('L2 Abs Var'))
        acc=acc.with_columns((1-acc['L2 Abs Var']/acc['`Act Orders Rev']).alias('L2 Acc'))
        acc=acc.with_columns(pl.when(acc['`Act Orders Rev']==0).then(1).otherwise(acc['L2 Acc']))
        acc=acc.with_columns(acc['L2 Acc'].clip(0,acc['L2 Acc'].max()).alias('L2 Acc'))
        acc1=acc.filter((acc['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=3)) & (acc['SALES_DATE']<=datetime(today.year,today.month,1)-relativedelta(months=1)))
        acc1=acc1.with_columns((acc1['`Act Orders Rev']/acc1['`Act Orders Rev'].sum()*100).alias('orders cont'))
        acc1=acc1.with_columns((acc1['L2 Abs Var']/acc1['L2 Abs Var'].sum()*100).alias('var cont'))
        acc1=acc1[['CatalogNumber','SALES_DATE','orders cont','var cont']]
        acc=acc.join(acc1,on=['CatalogNumber','SALES_DATE'],how='left')
        acc=acc[['CatalogNumber','SALES_DATE','`Act Orders Rev',l0fc,l2fc,'orders cont','var cont']]
        acc=acc.melt(['CatalogNumber','SALES_DATE','orders cont','var cont'])
        #acc.sort_values('`Act Orders Rev',ascending=False)
        its=alt.selection_point(fields=['CatalogNumber'])
        c1=alt.Chart(acc.filter(acc['SALES_DATE']==datetime(today.year,today.month,1)-relativedelta(months=1)).to_pandas()).mark_circle(color='#26A69A',size=85).encode(
            x=alt.X('sum(orders cont):Q',scale=alt.Scale(type="pow",exponent=0.3)),y=alt.Y('sum(var cont):Q',scale=alt.Scale(type="pow",exponent=0.3))
            ,tooltip=['CatalogNumber:O','orders cont','var cont']
            ,opacity=alt.condition(its, alt.value(.8), alt.value(0.18))).properties(height=400,width=600).add_params(its)
        c2=alt.Chart(acc.to_pandas()).mark_line(point=True).encode(x='SALES_DATE:T',y='sum(value):Q',color='variable',tooltip=['sum(value):Q','SALES_DATE']).properties(height=390,width=600).transform_filter(its)
        ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
        chart=c1|c2+ll
        print("PLOT Contribution!!")
        print(its)
        return chart.to_json(format="vega")

def cort(df,l0fc,fci,cc):  ##POLARS
    if not df.is_empty():
        today=datetime.today()
        df1=df.filter(df['CatalogNumber'].is_in(cc))
        df1=df1[['CatalogNumber','SALES_DATE','`Act Orders Rev','`Fcst Stat Final Rev']]
        #df1['SALES_DATE']=pd.to_datetime(df1['SALES_DATE'])
        df2=df1.filter((df1['SALES_DATE']<=datetime(today.year, today.month, 1)-relativedelta(months=1)) & (df1['SALES_DATE']>=datetime(today.year, today.month, 1)-relativedelta(months=25)))
        df2=df2.pivot(columns='CatalogNumber',index='SALES_DATE',values='`Act Orders Rev') #use unstack
        df3=df2.select(pl.exclude('SALES_DATE'))
        df3=df3.corr()
        df3=df3.with_columns(pl.Series(name="CatalogNumber", values=df3.columns))
        df3=df3.melt('CatalogNumber')
        df3=df3.filter(pl.col('value')!=0)
        df3=df3.rename({'value':'Correlation','variable':'CatalogNumber1'})  
        df4=df3.filter((abs(df3['Correlation'])>=.93) & (df3['CatalogNumber']!=df3['CatalogNumber1']))
        for i,dat in enumerate(df4.iter_rows()):
            tmp=df4.filter(df4['CatalogNumber']==dat[0])
            df4=df4.filter(~df4['CatalogNumber1'].is_in(tmp['CatalogNumber']))
            
        fc=df1.filter((df1['SALES_DATE']>=datetime(today.year, today.month, 1)) & (df1['SALES_DATE']<=datetime(today.year, today.month, 1)+relativedelta(months=12)))
        chl=[]
        tdf=df1.filter(df1['SALES_DATE']<=datetime(today.year, today.month, 1))
        tdf=tdf.sort('SALES_DATE')
        for i,dat in enumerate(df4.iter_rows()):
            #c1,c2,c3,c4='','','',''
            tdf1=tdf.filter(tdf['CatalogNumber']==dat[0]).sort('SALES_DATE')
            tdf2=tdf.filter(tdf['CatalogNumber']==dat[1]).sort('SALES_DATE')
            fc1=fc.filter(fc['CatalogNumber']==dat[0]).sort('SALES_DATE')
            fc2=fc.filter(fc['CatalogNumber']==dat[1]).sort('SALES_DATE')
            c1=alt.Chart(tdf1.to_pandas(),title=f'{dat[0]} vs {dat[1]}').mark_line(color='#E91E63').encode(x='SALES_DATE:T', y='sum(`Act Orders Rev):Q',opacity=alt.value(0.75),
                                                                                                        tooltip=['SALES_DATE:T','sum(`Act Orders Rev):Q','CatalogNumber:O'])
            c2=alt.Chart(tdf2.to_pandas()).mark_line(color='#42A5F5').encode(x='SALES_DATE:T', y='`Act Orders Rev',opacity=alt.value(0.75),
                                                                    tooltip=['SALES_DATE:T','sum(`Act Orders Rev):Q','CatalogNumber:O'])
            c3=alt.Chart(fc1.to_pandas()).mark_line(color='#E91E63').encode(x='SALES_DATE:T', y=alt.Y('`Fcst Stat Final Rev','sum',title='Fcst '+ 'Stat'), opacity=alt.value(0.75),
                                                                tooltip=['SALES_DATE:T',f'sum(`Fcst Stat Final Rev):Q','CatalogNumber:O'])
            c4=alt.Chart(fc2.to_pandas()).mark_line(color='#42A5F5').encode(x='SALES_DATE:T', y=alt.X('`Fcst Stat Final Rev','sum',title='Fcst '+ 'Stat'), opacity=alt.value(0.75),
                                                                tooltip=['SALES_DATE:T',f'sum(`Fcst Stat Final Rev):Q','CatalogNumber:O'])
            chl.append(c1+c2+c3+c4)
        return alt.concat(*chl, columns=5).to_json(format="vega")

def covt(df,l2fc,l0fc,fci,cc): ##POLARS
    if not df.is_empty():
        today=datetime.today()
        tcv=pl.DataFrame()
        tmdf=df.filter((df['SALES_DATE']>=datetime(today.year, today.month, 1)-relativedelta(months=12)) &  (df['SALES_DATE']<datetime(today.year, today.month, 1)))
        tcv=tcv.with_columns(tmdf.groupby('CatalogNumber').agg(pl.col('`Act Orders Rev').std().alias('std')))
        tcv=tcv.with_columns(tmdf.groupby('CatalogNumber').agg(pl.col('`Act Orders Rev').mean().alias('avg')))
        tcv=tcv.with_columns((tcv['std']/tcv['avg']).alias('cvar'))
        #tcv.reset_index(inplace=True)
        cv=df.join(tcv,on='CatalogNumber')
        cv=cv.filter(cv['CatalogNumber'].is_in(cc))
        cv=cv.sort('SALES_DATE')
        #cv=cv.with_columns(((cv['`Act Orders Rev']-cv['L2 Stat Final Rev'])/cv['`Act Orders Rev']).alias('acc'))
        cv=cv.with_columns(pl.when(cv['`Act Orders Rev']==0).then(1).otherwise((cv['`Act Orders Rev']-cv[l2fc])/cv['`Act Orders Rev']).alias('acc'))
        cv=cv.with_columns(pl.col('acc').clip(0,cv['acc'].max()))
        cv=cv.with_columns(pl.col('acc').shift(1).over('CatalogNumber').alias('l1macc'))
        cv=cv.with_columns(pl.col('acc').shift(2).over('CatalogNumber').alias('l2macc'))
        #cv['l2macc']=cv.groupby('CatalogNumber')['acc'].shift(2)
        cv=cv.with_columns((cv[['acc','l1macc','l2macc']].sum(axis=1)/3).alias('3macc'))
        #cv=cv.filter(cv['SALES_DATE']==datetime(today.year, today.month, 1))
        cv=cv.with_columns(pl.when(cv['cvar']>70).then(70).otherwise(cv['cvar']).alias('cvar'))
        #cv['l1macc']=cv['l1macc'].clip(0,None)
        #cv['l2macc']=cv['l2macc'].clip(0,None)
        #cv['3macc']=cv[['acc','l1macc','l2macc']].sum(axis=1)/3 
        print(cv.columns)
        cv1=cv[['CatalogNumber','IBP Level 5','SALES_DATE','`Act Orders Rev',l0fc,l2fc]]
        cv1=cv1.melt(['CatalogNumber','SALES_DATE','IBP Level 5'])
        its1=alt.selection_point(fields=['CatalogNumber'],nearest=True)
        f1=alt.Chart(cv.filter(cv['SALES_DATE']==datetime(today.year, today.month, 1)-relativedelta(months=1)).to_pandas()).mark_circle(color='#26A69A').encode(
            x=alt.X('mean(cvar):Q'),y=alt.Y('mean(3macc):Q',scale=alt.Scale(type="pow",exponent=0.4)),size=alt.Size('`Act Orders Rev:Q', scale=alt.Scale(range=[100,700])),
            tooltip=['CatalogNumber:O','IBP Level 5:O']
            ,opacity=alt.condition(its1, alt.value(.8), alt.value(0.18))).properties(height=400,width=600).add_params(its1)
        f2=alt.Chart(cv1.to_pandas()).mark_line(point=True).encode(x='SALES_DATE:T',y='sum(value):Q',color='variable:O',tooltip=['SALES_DATE','sum(value):Q']).properties(
            height=390,width=600).transform_filter(its1)
        #f2=alt.Chart(cv.to_pandas()).mark_line(point=True).encode(x='SALES_DATE:T',y='sum(`Act Orders Rev):Q',tooltip=['SALES_DATE','sum(`Act Orders Rev):Q']).properties(
        #    height=390,width=600).transform_filter(its1)
        #f3=alt.Chart(cv.to_pandas()).mark_line(color='#42A5F5',point=True).encode(x='SALES_DATE:T',y=alt.Y(l2fc,'sum',title='L2 '+fci),tooltip=['SALES_DATE',f'sum({l2fc}):Q']).transform_filter(its1)
        #f4=alt.Chart(cv.to_pandas()).mark_line(color='#26A69A',point=True).encode(x='SALES_DATE:T',y=alt.Y(l0fc,'sum',title='L0 '+fci),tooltip=['SALES_DATE',f'sum({l0fc}):Q']).transform_filter(its1)
        ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
        print("PLOT Covariance !!!")
        return (f1|(f2+ll)).to_json(format="vega")

def acct(df,l2fc,cc,ski):  #POLARS
    if not df.is_empty():
        today=datetime.today()
        acc=df.clone()
        acc=acc.with_columns(abs(acc['`Act Orders Rev']-acc[l2fc]).alias('L2 Abs Var'))
        acc=acc.with_columns((1-acc['L2 Abs Var']/acc['`Act Orders Rev']).alias('L2 Acc'))
        acc=acc.with_columns(pl.when(acc['`Act Orders Rev']==0).then(1).otherwise(acc['L2 Acc']).alias('L2 Acc'))
        acc=acc.with_columns(acc['L2 Acc'].clip(0,acc['L2 Acc'].max()).alias('L2 Acc'))
        acc2=acc.sort(['CatalogNumber','SALES_DATE'])
        acc2=acc2.with_columns(acc2['L2 Acc'].diff().alias('Decrease'))
        acc3=acc2.filter(acc2['SALES_DATE']==datetime(today.year,today.month,1)-relativedelta(months=1))
        acc3=acc3.filter(acc3['CatalogNumber'].is_in(cc[:ski]))  # Parameterize
        acc3=acc3.filter(acc3['Decrease']<0).sort('Decrease')
        its2=alt.selection_point(fields=['CatalogNumber'])
        f5=alt.Chart(acc3[['CatalogNumber','Decrease','IBP Level 5']].to_pandas()).mark_bar(color='#26A69A').encode(
            x=alt.X('CatalogNumber:O',sort='y'),y=alt.Y('mean(Decrease):Q'),tooltip=['CatalogNumber:O','IBP Level 5:O']
            ,opacity=alt.condition(its2, alt.value(.8), alt.value(0.18))).properties(height=390,width=600).add_params(its2)
        f6=alt.Chart(acc2[['SALES_DATE','`Act Orders Rev','CatalogNumber']].groupby(['SALES_DATE','CatalogNumber']).sum().to_pandas()).mark_line(point=True,color='#42A5F5').encode(x='SALES_DATE:T',y='sum(`Act Orders Rev):Q',tooltip=['SALES_DATE','sum(`Act Orders Rev):Q']).properties(
            height=390,width=700).transform_filter(its2)
        f7=alt.Chart(acc2[['SALES_DATE',l2fc,'CatalogNumber']].groupby(['SALES_DATE','CatalogNumber']).sum().to_pandas()).mark_line(point=True,color='#26A69A').encode(x='SALES_DATE:T',y=f'sum({l2fc}):Q',tooltip=['SALES_DATE',f'sum({l2fc}):Q']).transform_filter(its2)
        #c51=alt.Chart(acc).mark_text(align='left',fontSize=15).encode(text='CatalogNumber:O').transform_filter(its2)
        ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
        print('PLOT Accuracy!!')
        return (f5|(f6+f7+ll)).to_json(format="vega")

def ltt(df):
    if not df.is_empty():
        today=datetime.today()
        ltdf=df.clone()
        ltdf=ltdf.with_columns(pl.col("SALES_DATE").dt.strftime("%Y").alias("year"))
        ltdf=ltdf.with_columns(pl.when(pl.col('SALES_DATE')<datetime(today.year, today.month, 1)-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(pl.col('`Fcst DF Final Rev')).alias('actwfc'))
        ltdf=ltdf.with_columns(pl.when(pl.col('SALES_DATE')<datetime(today.year, today.month, 1)-relativedelta(months=1)).then(pl.col('Act Orders Rev Val')).otherwise(pl.col('Fcst DF Final Rev Val')).alias('actwfc val'))
        #df.loc[df['SALES_DATE']<pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True),'actwfc Val']=df['Act Orders Rev Val']
        #df.loc[df['SALES_DATE']>=pd.Timestamp.today()-pd.offsets.MonthBegin(1,normalize=True),'actwfc Val']=df['Fcst DF Final Rev Val']
        ltdf1=ltdf.groupby(['year','IBP Level 5']).sum()
        ltdf1=ltdf1[['year','IBP Level 5','actwfc','actwfc val']].melt(['year','IBP Level 5'])
        ch=alt.Chart(ltdf1[['year','value','variable']].to_pandas()).mark_bar(color='#26A69A').encode(x='year:O',y='sum(value):Q',color=alt.Color('variable').legend(orient="right"),tooltip=['year','sum(value):Q'],xOffset='variable')
        ch1=alt.Chart(ltdf1[['year','value','IBP Level 5','variable']].to_pandas()).mark_bar(color='#26A69A').encode(x='year:O',y='sum(value):Q',color='variable',xOffset='variable',tooltip=['year','sum(value):Q']).facet('IBP Level 5:O',columns = 7)
        return (ch&ch1).resolve_scale(y='independent').to_json(format="vega")
        
def fct(df,l2fc,l0fc): ## POLARS
    if not df.is_empty():
        today=datetime.today()
        fc=df.clone()
        fc=fc.filter((fc['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=1)) & (fc['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)))
        fc=fc.with_columns((fc['L2 DF Final Rev']-fc['`Fcst DF Final Rev']).alias('change'))
        fc=fc.with_columns(abs(fc['change']).alias('change'))
        fcg=fc.groupby(['CatalogNumber','SALES_DATE']).sum().sort('SALES_DATE')
        fcg=fcg[['CatalogNumber','SALES_DATE','`Act Orders Rev',l0fc,l2fc,'change']]
        #fcg=fcg.melt(['CatalogNumber','SALES_DATE'])
        fc1=df.clone()
        fc1=fc1.groupby(['CatalogNumber','SALES_DATE']).sum().sort('SALES_DATE')
        fc1=fc1.join(fcg[['CatalogNumber','SALES_DATE','change']],on=['CatalogNumber','SALES_DATE'],how='left')
        fc1=fc1[['CatalogNumber','SALES_DATE','`Act Orders Rev',l0fc,l2fc,'change']]
        fc1=fc1.melt(['CatalogNumber','SALES_DATE','change'])
        its=alt.selection_point(fields=['CatalogNumber'])
        c1=alt.Chart(fc1.sort('change',descending=True).head(150).to_pandas()).mark_bar(color='#26A69A').encode(x=alt.X('CatalogNumber:O',sort='-y'),y=alt.Y('sum(change):Q'),tooltip=['CatalogNumber:O'] #PARAMETERIZE
            ,opacity=alt.condition(its, alt.value(.8), alt.value(0.18))).properties(height=400,width=600).add_params(its)
        c2=alt.Chart(fc1.groupby(['SALES_DATE','variable','CatalogNumber']).sum().to_pandas()).mark_line(point=True).encode(x='SALES_DATE', y='sum(value):Q',opacity=alt.value(0.75),color='variable',
                                                                    tooltip=['SALES_DATE','sum(value):Q']).properties(height=390,width=600).transform_filter(its)
        #c3=alt.Chart(fc1).mark_line(color='#E91E63').encode(x='SALES_DATE', y=alt.Y(l2fc,'sum',title='L2 '+fci), opacity=alt.value(0.75),
        #                                                    tooltip=['SALES_DATE',f'sum({l2fc})']).transform_filter(its)
        #c4=alt.Chart(fc1).mark_line(color='#42A5F5').encode(x='SALES_DATE', y=alt.X(l0fc,'sum',title='Fcst '+fci), opacity=alt.value(0.75),
        #                                                    tooltip=['SALES_DATE',f'sum({l0fc})']).transform_filter(its)
        ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
        return (c1|c2+ll).to_json(format="vega")

@component
def App():
    fr,set_fri=use_state("CMF")
    co,set_coi=use_state("INDIA")
    fci,set_fci=use_state("Stat Fcst")
    ski,set_ski=use_state(150)
    dfi,set_dfi=use_state(pl.DataFrame())
    uni,set_uni=use_state('')
    pwi,set_pwi=use_state('')
    pi,set_pi=use_state("password")
    sl,set_sl=use_state(" slash")
    ch,set_ch=use_state("")
    #cht,set_cht=use_state("Accuracy")
    #print(ski)
    if not dfi.is_empty():
        #try:
        #cc=dfi.groupby('CatalogNumber').sum(numeric_only=True)[['`Act Orders Rev']].sort_values(ascending=False,by='`Act Orders Rev')[:ski].index
        cc=dfi.groupby('CatalogNumber').agg(pl.sum('`Act Orders Rev')).sort('`Act Orders Rev',descending=True)['CatalogNumber'][:ski]
        #except:
        #    cc=dfi.groupby('CatalogNumber').sum(numeric_only=True)[['`Act Orders Rev']].sort_values(ascending=False,by='`Act Orders Rev').index
    else:
        cc=''
    try:
        with open('config.json') as json_file:
            fdata = json.load(json_file)
            un = fdata.get('username')
            pp = fdata.get('password')
            set_uni(un)
            set_pwi(pp)
    except:
        pass

    if fci=='Stat Fcst':
        l2fc='L2 Stat Final Rev'
        l0fc='`Fcst Stat Final Rev'
    else:
        l2fc='L2 DF Final Rev'
        l0fc='`Fcst DF Final Rev'

    def sabf(e):
        cred={'username':uni,'password':pwi}
        with open('config.json', 'w+') as outfile:
            outfile.write(json.dumps(cred))
    def frf(e):
        set_fri(e["target"]["value"])
    def cof(e):
        set_coi(e["target"]["value"])
    def fcf(e):
        set_fci(e["target"]["value"])
    def lpr(e):
        print(co,fr)
        df=pl.read_parquet(f'{[co]}-{[fr]}.parquet')
        set_dfi(df)
        print(dfi)
    def myf(e):
        if pi=="password":
            set_pi("text")
            set_sl('')
        else:
            set_pi("password")
            set_sl(" slash")
    def enbf(e):
        df=data(co,[fr],uni,pwi)
        df.write_parquet(f'{[co]}-{[fr]}.parquet')        

    #cht='Accuracy'
    #ch=''
    def pch(e):
        print(e['target']['value'])
        if e['target']['value']=='Correlation':
            set_ch(cort(dfi,l0fc,fci,cc))
        elif e['target']['value']=='Contribution':
            set_ch(cont(dfi,l2fc,l0fc,fci,cc))
        elif e['target']['value']=='COV':
            set_ch(covt(dfi,l2fc,l0fc,fci,cc))
        elif e['target']['value']=='Accuracy':
            set_ch(acct(dfi,l2fc,cc,ski))
        elif e['target']['value']=='Change':
            set_ch(fct(dfi,l2fc,l0fc))
        elif e['target']['value']=='Long Term':
            set_ch(ltt(dfi))
        #elif e['target']['value']=='Download':
        #    show()
        return ch


    k=html.div(html.div({"class_name":"ui"},
               html.div({"class_name":"ui styled fluid accordion"},
               html.div({"class_name":"title"}, html.i({"class_name":"dropdown icon"}),"Credentials"),
               html.div({"class_name":"ui form content"},
                         html.div({"class_name":"ui three fields transition"},
                                  html.div({"class_name":"field"},
                        html.input({"placeholder":"User Name","class_name":"ui input","value":uni,"on_change":lambda e: set_uni(e['target']['value'])})),
                        html.div({"class_name":"field"},
                        html.input({"placeholder":"Password","type":pi,"class_name":"ui input attached field","id":"myInput","value":pwi,"on_change":lambda e: set_pwi(e['target']['value'])})),
                        html.i({"on_click":myf,"class":"eye attached icon"+sl,"style":{"margin-left": "4px", "cursor": "pointer"},"id":"togglePassword"}),
                        html.div({"class_name":"field"},
                        html.button({"on_click":sabf,"class_name":"ui primary button field"},"Save"))))),
                        html.div({"class_name":"ui form basic segment"},
                         html.div({"class_name":"six fields"},
                                  html.div({"class_name":"field"},
                                  html.select({"class_name":"ui selection dropdown","value":fci,"on_change":fcf},
                                           html.i({"class_name":"dropdown icon"}),
                                  html.option({"class_name":"item","value":"Stat Fcst"},'Stat Fcst'),
                                  html.option({"class_name":"item","value":"DF Fcst"},'DF Fcst')
                                  )),
                                  html.div({"class_name":"field"},
                                  html.select({"class_name":"ui fluid dropdown","value":fr,"on_change":frf},
                                           html.div({"class_name":"ui text"},"Franchise"),
                                           html.i({"class_name":"dropdown icon"}),
                                  [html.option({"class_name":"item","value":i},i) for i in fran],
                                  )),
                                  html.div({"class_name":"field"},
                                  html.select({"class_name":"ui selection dropdown","value":co,"on_change":cof},
                                           html.div({"class_name":"ui text"},"Country"),
                                           html.i({"class_name":"dropdown icon"}),
                                    [html.option({"class_name":"item","value":i},i) for i in coun],
                                  )),
                                  html.div({"class_name":"field"},
                                  html.input({"class_name":"ui text","type":"number","value":ski,"on_change":lambda e: set_ski(eval(e['target']['value']))}),
                                  ),
                                  html.div({"class_name":"field"},
                                  html.button({"class_name":"ui primary button", "on_click":enbf},"Get Envision"),
                                  html.button({"class_name":"ui primary button", "on_click":lpr},"Load Local")
                                  ))),
                                  html.div({"class_name":"ui menu"},
                    html.button({"class_name":"ui button","on_click":pch,'value':"Accuracy"},"Accuracy"),
                    html.button({"class_name":"ui button","on_click":pch,'value':"Change"},"Change"),
                    html.button({"class_name":"ui button","on_click":pch,'value':"Long Term"},"Long Term"),
                    html.button({"class_name":"ui button","on_click":pch,'value':"Correlation"}, "Correlation"),
                    html.button({"class_name":"ui button","on_click":pch,'value':"COV"}, "COV"),
                    html.button({"class_name":"ui button","on_click":pch,'value':"Contribution"}, "Contribution")),
                    html.div({"id":"vis3"}),
                    html.button({"class_name":"ui button","on_click":pch,'value':"Download"}, "Download"),
                    html.script({"language":"javascript"},f'''vegaEmbed('#vis3', {ch});'''),
                    ),
                            html.script({"language":"javascript"},"$('.ui.accordion').accordion();$('.ui.dropdown').dropdown();$('.tabular.menu .item').tab();"))
    return k

#run(App)
app = Sanic("MyHelloWorldApp")
configure(app,App,Options(head=headv))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info")