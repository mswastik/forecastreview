import altair as alt
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import polars as pl
import panel as pn
import param

alt.data_transformers.disable_max_rows()
alt.renderers='svg'

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
                "category": ["#FFCC33", "#B2B4AE", "#85458A", "#4C7D7A", "#545857", "#4C7D7A", "#FF6692", "#B6E880", "#FF97FF", "#FECB52"],
                "diverging": ["#FFCC33", "#B2B4AE", "#85458A", "rgb(153,204,255)", "rgb(204,204,255)", "rgb(255,255,255)", "rgb(255,204,255)", "rgb(255,153,255)", "rgb(255,102,204)", "rgb(255,102,102)", "rgb(255,0,0)"],
                "heatmap": ['rgb(0,0,255)', 'rgb(51,153,255)', 'rgb(102,204,255)', 'rgb(153,204,255)', 'rgb(204,204,255)', 'rgb(255,255,255)', 'rgb(255,204,255)', 'rgb(255,153,255)', 'rgb(255,102,204)', 'rgb(255,102,102)', 'rgb(255,0,0)'],
                "ramp": ['#4C7D7A', '#545857', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'],
                "ordinal": ["#FFCC33", "#B2B4AE", "#85458A", "#4C7D7A", "#545857", "#4C7D7A",'#636EFA', '#EF553B', '#00CC96', '#AB63FA', '#FFA15A', '#19D3F3', '#FF6692', '#B6E880', '#FF97FF', '#FECB52'],
             },
               }
    }

alt.themes.register("alt_theme", alt_theme)
alt.themes.enable("alt_theme")

cwd="C:\\Users\\smishra14\\OneDrive - Stryker\\python\\app"

class B(param.Parameterized):
    df=param.DataFrame()
    sdf=pd.read_parquet(f"{cwd}\\{['INDIA']}-{['Joint Replacement']}.parquet")
    sdf=sdf[['Country','IBP Level 5','CatalogNumber']]
    sdf=sdf.drop_duplicates()
    sdf=param.DataFrame(sdf)
    rea=''
b=B()

def filtered_table(selection):  ##FILTERED DF
    if (not selection):
        pass
    else:
        b.df=b.sdf[b.sdf['CatalogNumber']==selection[0]['CatalogNumber']]
        b.df['Reason']=b.rea

def dt():
    return b.df
today=datetime.today()

def cont(df,l2fc,l0fc,cc,cp):  #POLARS
    #acc.sort_values('`Act Orders Rev',ascending=False)
    print(df)
    #df=df.to_pandas(df)
    its=alt.selection_point(fields=['CatalogNumber'],name='brush')
    c1=alt.Chart(df[df['SALES_DATE']==datetime(today.year,today.month,1)-relativedelta(months=2)]).mark_circle(size=85).encode(
        x=alt.X('sum(orders cont):Q',scale=alt.Scale(type="pow",exponent=0.3)),y=alt.Y('sum(var cont):Q',scale=alt.Scale(type="pow",exponent=0.3))
        ,tooltip=['CatalogNumber:O','orders cont','var cont']
        ,opacity=alt.condition(its, alt.value(.8), alt.value(0.18))).properties(height=400,width=600).add_params(its)
    c2=alt.Chart(df).mark_line(point=True).encode(alt.X('SALES_DATE:T', axis=alt.Axis(format="%b-%y")),y='sum(value):Q',color='type',tooltip=['sum(value):Q','SALES_DATE']).properties(height=390,width=600).transform_filter(its)
    ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
    chart=c1|c2+ll
    print("PLOT Contribution!!")
    #return chart.to_json(format="vega") #use with vegafusion
    b.rea='Contribution'
    sel=pn.pane.Vega(chart, debounce=10)
    cp.append(pn.bind(filtered_table,sel.selection.param.brush))
    #print(sel.selection)
    return sel
    
def cort(df,l0fc,fci,cc,cp):  ##POLARS
    df=pl.from_pandas(df)
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
        its=alt.selection_point(fields=['CatalogNumber'],name='brush')
        for i,dat in enumerate(df4.iter_rows()):
            #c1,c2,c3,c4='','','',''
            tdf1=tdf.filter(tdf['CatalogNumber']==dat[0]).sort('SALES_DATE')
            tdf2=tdf.filter(tdf['CatalogNumber']==dat[1]).sort('SALES_DATE')
            fc1=fc.filter(fc['CatalogNumber']==dat[0]).sort('SALES_DATE')
            fc2=fc.filter(fc['CatalogNumber']==dat[1]).sort('SALES_DATE')
            c1=alt.Chart(tdf1.to_pandas(),title=f'{dat[0]} vs {dat[1]}').mark_line(color='#FFCC33').encode(x='SALES_DATE:T', y='sum(`Act Orders Rev):Q',opacity=alt.value(0.75),
                                                                                                        tooltip=['SALES_DATE:T','sum(`Act Orders Rev):Q','CatalogNumber:O']).add_params(its)
            c2=alt.Chart(tdf2.to_pandas()).mark_line().encode(x='SALES_DATE:T', y='`Act Orders Rev',opacity=alt.value(0.75),
                                                                    tooltip=['SALES_DATE:T','sum(`Act Orders Rev):Q','CatalogNumber:O']).add_params(its)
            c3=alt.Chart(fc1.to_pandas()).mark_line(color='#FFCC33').encode(x='SALES_DATE:T', y=alt.Y('`Fcst Stat Final Rev','sum',title='Fcst '+ 'Stat'), opacity=alt.value(0.75),
                                                                tooltip=['SALES_DATE:T',f'sum(`Fcst Stat Final Rev):Q','CatalogNumber:O'])
            c4=alt.Chart(fc2.to_pandas()).mark_line().encode(x='SALES_DATE:T', y=alt.X('`Fcst Stat Final Rev','sum',title='Fcst '+ 'Stat'), opacity=alt.value(0.75),
                                                                tooltip=['SALES_DATE:T',f'sum(`Fcst Stat Final Rev):Q','CatalogNumber:O'])
            chl.append(c1+c2+c3+c4)
        b.rea='Correlation'
        sel=pn.pane.Vega(alt.concat(*chl, columns=5), debounce=10)
        cp.append(pn.bind(filtered_table,sel.selection.param.brush))
        return sel

def covt(df,l2fc,l0fc,cc,cp): ##POLARS
    df=pl.from_pandas(df)
    if not df.is_empty():
        today=datetime.today()
        tcv=pl.DataFrame()
        tmdf=df.filter((df['SALES_DATE']>=datetime(today.year, today.month, 1)-relativedelta(months=12)) &  (df['SALES_DATE']<datetime(today.year, today.month, 1)))
        tcv=tcv.with_columns(tmdf.groupby('CatalogNumber').agg(pl.col('`Act Orders Rev').std().alias('std')))
        tcv1=tcv.with_columns(tmdf.groupby('CatalogNumber').agg(pl.col('`Act Orders Rev').mean().alias('avg')))
        tcv=tcv.join(tcv1[['CatalogNumber','avg']],on='CatalogNumber')
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
        cv=cv.with_columns(pl.when(pl.col('cvar')>5).then(5).otherwise(pl.col('cvar')).alias('cvar'))
        cv=cv.with_columns(pl.when(pl.col('avg')==0).then(0).otherwise(pl.col('cvar')).alias('cvar'))
        #cv['l1macc']=cv['l1macc'].clip(0,None)
        #cv['l2macc']=cv['l2macc'].clip(0,None)
        #cv['3macc']=cv[['acc','l1macc','l2macc']].sum(axis=1)/3 
        print(cv.columns)
        cv1=cv[['CatalogNumber','IBP Level 5','SALES_DATE','`Act Orders Rev',l0fc,l2fc]]
        cv1=cv1.melt(['CatalogNumber','SALES_DATE','IBP Level 5'])
        its1=alt.selection_point(fields=['CatalogNumber'],name='brush')
        f1=alt.Chart(cv.filter(cv['SALES_DATE']==datetime(today.year, today.month, 1)-relativedelta(months=1)).to_pandas()).mark_circle(color='#26A69A').encode(
            x=alt.X('mean(cvar):Q'),y=alt.Y('mean(3macc):Q',scale=alt.Scale(type="pow",exponent=0.4)),size=alt.Size('`Act Orders Rev:Q', scale=alt.Scale(range=[100,700])),
            tooltip=['CatalogNumber:O','IBP Level 5:O','std','avg']
            ,opacity=alt.condition(its1, alt.value(.8), alt.value(0.18))).properties(height=400,width=600).add_params(its1)
        f2=alt.Chart(cv1.to_pandas()).mark_line(point=True).encode(x='SALES_DATE:T',y='sum(value):Q',color='variable',tooltip=['SALES_DATE','sum(value):Q']).properties(
            height=390,width=600).transform_filter(its1)
        ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
        print("PLOT Covariance !!!")
        b.rea='COV'
        sel=pn.pane.Vega(alt.concat((f1|(f2+ll)), columns=4), debounce=10)
        cp.append(pn.bind(filtered_table,sel.selection.param.brush))
        return sel

def acct(df,l2fc,cc,ski,cp):  #POLARS
    its2=alt.selection_point(fields=['CatalogNumber'],name='brush')
    f5=alt.Chart(df[['CatalogNumber','Decrease','IBP Level 5']]).mark_bar().encode(
        x=alt.X('CatalogNumber:O',sort='y'),y=alt.Y('mean_acc:Q'),tooltip=['CatalogNumber:O','IBP Level 5:O']
        ,opacity=alt.condition(its2, alt.value(.8), alt.value(0.18))).transform_aggregate(mean_acc='mean(Decrease)',groupby=["CatalogNumber","IBP Level 5"]).properties(height=390,width=600).add_params(its2)
    f6=alt.Chart(df).mark_line(point=True).encode(x=alt.X('SALES_DATE:T', axis=alt.Axis(format="%b-%y")),y='sum(value):Q',color='type',
                                                tooltip=['SALES_DATE','sum(value):Q']).properties(height=390,width=600).transform_filter(its2)
    ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
    print('PLOT Accuracy!!')
    #print(f5.transformed_data())
    b.rea='Accuracy'
    sel=pn.pane.Vega(f5|(f6+ll), debounce=10)
    cp.append(pn.bind(filtered_table,sel.selection.param.brush))
    return sel

def ltt(df,cp):
    df=pl.from_pandas(df)
    if not df.is_empty():
        today=datetime.today()
        ltdf=df.clone()
        ltdf=ltdf.with_columns(pl.col("SALES_DATE").dt.strftime("%Y").alias("year"))
        ltdf=ltdf.with_columns(pl.when(pl.col('SALES_DATE')<datetime(today.year, today.month, 1)-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(pl.col(
                                        '`Fcst DF Final Rev')).alias('actwfc'))
        ltdf=ltdf.with_columns(pl.when(pl.col('SALES_DATE')<datetime(today.year, today.month, 1)-relativedelta(months=1)).then(pl.col('Act Orders Rev Val')).otherwise(pl.col(
                                        'Fcst DF Final Rev Val')).alias('actwfc val'))
        ltdf1=ltdf.groupby(['year','IBP Level 5']).sum()
        ltdf1=ltdf1[['year','IBP Level 5','actwfc','actwfc val']].melt(['year','IBP Level 5'])
        its=alt.selection_point(fields=['CatalogNumber'],name='brush')
        ch=alt.Chart(ltdf1[['year','value','variable']].to_pandas()).mark_bar(color='#26A69A').encode(x='year:O',y='sum(value):Q',color=alt.Color('variable').legend(orient="right"),tooltip=['year','sum(value):Q'],xOffset='variable')
        ch1=alt.Chart(ltdf1[['year','value','IBP Level 5','variable']].to_pandas()).mark_bar(color='#26A69A').encode(x='year:O',y='sum(value):Q',color='variable',xOffset='variable',tooltip=['year','sum(value):Q']).facet('IBP Level 5:O',columns = 7)
        b.rea='Long Term'
        sel=pn.pane.Vega((ch&ch1).resolve_scale(y='independent'), debounce=10)
        #cp.append(pn.bind(filtered_table,sel.selection.param.brush))
        return sel
        
def fct(df,l2fc,l0fc,cp): ## POLARS
    #df=pl.from_pandas(df)
    #fc=df.clone()
    #fc=fc.filter((fc['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=1)) & (fc['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)))
    #fc=fc.with_columns((fc['L2 DF Final Rev']-fc['`Fcst DF Final Rev']).alias('change'))
    #fc=fc.with_columns(abs(fc['change']).alias('change'))
    #fcg=df.groupby(['CatalogNumber','SALES_DATE']).sum().sort('SALES_DATE')
    #fcg=fcg[['CatalogNumber','SALES_DATE','`Act Orders Rev',l0fc,l2fc,'change']]
    #fc1=df.clone()
    #fc1=fc1.groupby(['CatalogNumber','SALES_DATE']).sum().sort('SALES_DATE')
    #fc1=fc1.join(fcg[['CatalogNumber','SALES_DATE','change']],on=['CatalogNumber','SALES_DATE'],how='left')
    #fc1=fc1[['CatalogNumber','SALES_DATE','`Act Orders Rev',l0fc,l2fc,'change']]
    #fc1=fc1.melt(['CatalogNumber','SALES_DATE','change'])
    its=alt.selection_point(fields=['CatalogNumber'],name='brush')
    c1=alt.Chart(df.groupby('CatalogNumber').sum(numeric_only=True).sort_values('change',ascending=False).reset_index().head(150)).mark_bar(color='#26A69A').encode(x=alt.X('CatalogNumber:O',sort='-y'),y=alt.Y('sum(change):Q'),tooltip=['CatalogNumber:O'] #PARAMETERIZE
        ,opacity=alt.condition(its, alt.value(.8), alt.value(0.18))).properties(height=400,width=600).add_params(its)
    c2=alt.Chart(df.groupby(['SALES_DATE','type','CatalogNumber']).sum().reset_index()).mark_line(point=True).encode(x='SALES_DATE', y='sum(value):Q',opacity=alt.value(0.75),color='type',
                                                                tooltip=['SALES_DATE','sum(value):Q']).properties(height=390,width=600).transform_filter(its)
    ll = alt.Chart(pd.DataFrame({'Date': [pd.Timestamp.today()-pd.offsets.MonthBegin(2,normalize=True)]})).mark_rule().encode(x = 'Date:T')
    b.rea='Forecast Change'
    sel=pn.pane.Vega((c1|c2+ll), debounce=10)
    cp.append(pn.bind(filtered_table,sel.selection.param.brush))
    return sel
