from turbodbc import connect, make_options, Megabytes
options = make_options(read_buffer_size=Megabytes(300),
                        parameter_sets_to_buffer=1000,
                        varchar_max_character_limit=1000,
                        use_async_io=True,
                        prefer_unicode=True,
                        large_decimals_as_64_bit_types=True,
                        limit_varchar_results_to_max=True)

css = '''
    .widget-button .bk-btn-group button {
        font-size: 15px;}
    .bk-Button bk-btn-group bk-btn {
        font-size: 15px;}
    option {
        min-height: 15px; !important
        height:15;
        font-size: 15px;}      
    .bk-input-group .bk-input {
        font-size: 15px;}
    html, body {
        display: flow-root;
        box-sizing: border-box;
        height: 100%;
        margin: 0px;
        padding: 0px;
        min-height: 14px;
        font-size: 15px;}
    h3 {
        font-weight: normal;}
    .bk-btn-primary {
        background-color: #85458A !important; 
    }
    .bk-btn-warning {
        color: black !important; 
    }
'''
fran=['CMF','Instruments','Joint Replacement', 'Trauma and Extremities','Endoscopy','Spine']
coun=['INDIA', 'CHINA','UNITED STATES','JAPAN','AUSTRALIA']

em=['swastik.mishra@fff.com','pratik.h@fff.com','s.agarwal@fff.com']
