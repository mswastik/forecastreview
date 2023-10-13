from nicegui import Client,ui
from turbodbc import connect, make_options, Megabytes
from sql import dmt,cld
import polars as pl
from charts import *
import json
import win32com.client as win32
import nicegui as ng


ui.colors(primary='#FFCC33',accent='#4C7D7A',secondary='#85458A')
today=datetime.today()
class Demo:
        def __init__(self):
            self.co,self.fr,self.ft='INDIA','Joint Replacement','Stat'
            self.df=pl.DataFrame()
            self.df1=pl.DataFrame()
            self.cc=''
            self.ni=80
            self.ch='a'
            self.l2fc=f'L2 {self.ft} Final Rev'
            self.l0fc=f'`Fcst {self.ft} Final Rev'
            self.un=''
            self.pw=''
demo=Demo()

try:
    with open('config.json') as json_file:
        fdata = json.load(json_file)
        demo.un = fdata.get('username')
        demo.pw = fdata.get('password')
except:
    pass

options = make_options(read_buffer_size=Megabytes(300),
                        parameter_sets_to_buffer=1000,
                        varchar_max_character_limit=1000,
                        use_async_io=True,
                        prefer_unicode=True,
                        large_decimals_as_64_bit_types=True,
                        limit_varchar_results_to_max=True)

def data(e):
    ss="gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net"
    cnxn=connect(DRIVER='ODBC Driver 17 for SQL Server',server=ss,user=f'{demo.un}',password=demo.pw,database="gda_glbsyndb",Trusted_Connection='yes', turbodbc_options=options)
    if demo.fr in ['Instruments','Joint Replacement','Spine','Trauma and Extremities','Neurovascular']:
        query = dmt()
    elif demo.fr in ['CMF','Endoscopy','Sustainability']:
        query = cld()
    cur=cnxn.cursor()
    print('PULLING DATA!!')
    #spin.set_visiblity(True)
    cur.execute(query,[demo.fr,'','','','','', demo.co])
    dd=cur.fetchallarrow()
    df=pl.DataFrame(dd)
    df.write_parquet(f'{[demo.co]}-{[demo.fr]}.parquet')
    print('DONE!!')
    #spin.set_visiblity(True)
    cnxn.close()
    cur.close()

def sabf(e):
    cred={'username':demo.un,'password':demo.pw}
    with open('config.json', 'w+') as outfile:
        outfile.write(json.dumps(cred))

def lpr(e):
    #spin.set_visibility(True)
    df=pl.read_parquet(f'{[demo.co]}-{[demo.fr]}.parquet')
    df1=df.filter((df['SALES_DATE']>=datetime(today.year,today.month,1)-relativedelta(months=12)) & (df['SALES_DATE']<=datetime(today.year,today.month,1)+relativedelta(months=12)))
    df1=df1.with_columns(pl.when(df1['SALES_DATE']<datetime(today.year,today.month,1)).then(df1['`Act Orders Rev']).otherwise(df1['`Fcst DF Final Rev']).alias('actwfc'))
    cc=df1.groupby('CatalogNumber').agg(pl.sum('actwfc')).sort('actwfc',descending=True)['CatalogNumber'][:int(demo.ni)]
    demo.df1=df1
    demo.df=df
    demo.cc=cc
    #spin.set_visibility(False)

fran=['CMF','Instruments','Joint Replacement', 'Trauma and Extremities','Endoscopy','Spine']
coun=['INDIA', 'CHINA','UNITED STATES','JAPAN','AUSTRALIA']

@ui.page('/')
def home():
    ui.colors(primary='#FFCC33',accent='#4C7D7A',secondary='#85458A')
    ui.add_head_html('<script src="https://code.jquery.com/jquery-3.1.1.min.js",integrity="sha256-hVVnYaiADRTO2PzUGmuLJr8BLUSjGIZsDYGmIJLv2b8=",crossorigin="anonymous"></script>')  
    ui.add_head_html('<script src="https://cdn.bokeh.org/bokeh/release/bokeh-3.2.2.min.js",crossorigin="anonymous"></script>')
    ui.add_head_html('<script src="https://cdn.bokeh.org/bokeh/release/bokeh-widgets-3.2.2.min.js",crossorigin="anonymous"></script>')
    ui.add_head_html('<script src="https://cdn.bokeh.org/bokeh/release/bokeh-tables-3.2.2.min.js",crossorigin="anonymous"></script>')
    ui.add_head_html('<script src="https://cdn.bokeh.org/bokeh/release/bokeh-gl-3.2.2.min.js",crossorigin="anonymous"></script>')
    ui.add_head_html('<script src="https://cdn.bokeh.org/bokeh/release/bokeh-mathjax-3.2.2.min.js",crossorigin="anonymous"></script>')
    ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/vega@5"></script>')
    ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/vega-lite@5"}></script>')
    ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/vega-embed@6"}></script>')
    navigation = ui.header(bordered=True).classes(replace='row w-full item-center px-5').props('color="#FFCC33"').style("min-height: 40px; max-height: 50px")
    with navigation:
        with ui.link(target='/'):
            ui.button("Home").props('flat').tailwind.text_color('black')
        with ui.link(target='/list',new_tab=True):
            ui.button("List").props('flat').tailwind.text_color('black')
    with ui.row():
        with ui.expansion('Credentials').style("min-width: 540px"):
            with ui.row():
                ui.input('Username',value=demo.un).style("min-width: 300px")
                ui.input('Password',value=demo.pw,password=True,password_toggle_button=True).style("min-width: 200px")
        ui.select(label="Country",options=coun,value="INDIA").classes('col-md-auto').style("min-width: 260px").bind_value(demo,'co')
        ui.select(label="Franchise",options=fran,value="Joint Replacement").classes('col-4 col-md-auto').style("min-width: 260px").bind_value(demo,'fr')
        ui.button('Load Local',on_click=lpr).classes("mt-4")
        ui.button('Load Envision',on_click=data).classes("mt-4")
        spin=ui.spinner('dots', size='lg', color='purple-7')
        spin.visible=False
    
    def ade(e):
        pass
    
    def fct1(e):
        demo.cc=demo.df1.groupby('CatalogNumber').agg(pl.sum('actwfc')).sort('actwfc',descending=True)['CatalogNumber'][:int(demo.ni)]
        if demo.ft=='Stat':
            demo.l2fc='L2 Stat Final Rev'
            demo.l0fc='`Fcst Stat Final Rev'
        else:
            demo.l2fc='L2 DF Final Rev'
            demo.l0fc='`Fcst DF Final Rev'

    with ui.row():
        ui.select(options=['Stat','DF'],label="Type",value='Stat',on_change=fct1).style("min-width: 130px").bind_value(demo,'ft')
        ui.number(label="SKU Rank",value=80,on_change=fct1).classes('col-4 col-md-auto').style("min-width: 130px").bind_value(demo,'ni')
        ui.button('Add to Exception',on_click=ade).classes("mt-4")

    async def conp(e):
        demo.ch=cont(demo.df,demo.l2fc,demo.l0fc,demo.cc)
        #await ui.run_javascript(f'Bokeh.embed.embed_item({dd},"vis3")',respond=False)
        await ui.run_javascript(f'document.getElementById("head").innerHTML = "<h2>Contribution</h2>";vegaEmbed("#vis3", {demo.ch})',respond=False)
    async def accp(e):
        demo.ch=acct(demo.df,demo.l2fc,demo.cc,int(demo.ni))
        await ui.run_javascript(f'document.getElementById("head").innerHTML = "<h2>Accuracy</h2>";vegaEmbed("#vis3", {demo.ch})',respond=False)
    async def corp(e):
        demo.ch=cort(demo.df,demo.l2fc,demo.l0fc,demo.cc)
        await ui.run_javascript(f'document.getElementById("head").innerHTML = "<h2>Correlation</h2>";vegaEmbed("#vis3", {demo.ch})',respond=False)
    async def chap(e):
        demo.ch=fct(demo.df,demo.l2fc,demo.l0fc)
        await ui.run_javascript(f'document.getElementById("head").innerHTML = "<h2>Forecast Change</h2>";vegaEmbed("#vis3", {demo.ch})',respond=False)
    async def lonp(e):
        demo.ch=ltt(demo.df)
        await ui.run_javascript(f'document.getElementById("head").innerHTML = "Long Term";vegaEmbed("#vis3", {demo.ch})',respond=False)
    async def covp(e):
        demo.ch=covt(demo.df,demo.l2fc,demo.l0fc,demo.cc)
        await ui.run_javascript(f'document.getElementById("head").innerHTML = "<h2>COV</h2>";vegaEmbed("#vis3", {demo.ch})',respond=False)

    with ui.row():
        ui.button('Contribution',on_click=conp,color='secondary').props('outline')
        ui.button('Accuracy',on_click=accp,color='secondary').props('outline')
        ui.button('Correlation',on_click=corp,color='secondary').props('outline')
        ui.button('Change',on_click=chap,color='secondary').props('outline')
        ui.button('Long Term',on_click=lonp,color='secondary').props('outline')
        ui.button('COV',on_click=covp,color='secondary').props('outline')

    #ui.markdown(f'####{demo.head}####')
    ui.html(f'<div class="text-2xl" id="head"></div>')
    ui.html(f'<div id="vis3"></div>')

@ui.page('/list')
def chat():
    ui.colors(primary='#FFCC33',accent='#4C7D7A',secondary='#85458A')
    navigation = ui.header(bordered=True).classes(replace='row w-full item-center px-5').props('color="#FFCC33"').style("min-height: 40px; max-height: 50px")
    with navigation:
        with ui.link(target='/'):
            ui.button("Home").props('flat').tailwind.text_color('black')
        with ui.link(target='/list'):
            ui.button("List").props('flat').tailwind.text_color('black')
    com=pd.read_excel("C:\\Users\\smishra14\\OneDrive - Stryker\\Dashboard\\Comments.xlsx")
    com['Month']=pd.to_datetime(com['Month'])
    #com=pl.from_pandas(com)
    columns = [
    {'name': 'name', 'label': 'Name', 'field': 'name', 'required': True, 'align': 'left'},
    {'name': 'age', 'label': 'Age', 'field': 'age', 'sortable': True}]
    com.filter((com['Month']==datetime(today.year,today.month,1)) & (com['Franchise']==demo.fr) & (com['Country']==demo.co))
    print(com)
    ui.table(columns=com.columns,rows=com.to_dict(),row_key='CatalogNumber').props('no-data-label="No Data"')

ui.run(title='Insights',favicon='δ')