from Google import Create_Service # link to source code is in the description
import pypyodbc as odbc # pip install pypyodbc
import pandas as pd # pip install pandas

"""
Step 1.1 Connect to MS SQL Server Database System
"""
DRIVER_NAME = 'SQL Server'
SERVER_NAME = '<server_name>'
DATABASE_NAME = '<database_name>'

def connection_string(driver_name, server_name, database_name):
    # uid=<username>;
    # pwd=<password>;
    conn_string = f"""
        DRIVER={{{driver_name}}};
        SERVER={server_name};
        DATABASE={database_name};
        Trust_Connection=yes;        
    """
    return conn_string

try:
    conn = odbc.connect(connection_string(DRIVER_NAME, SERVER_NAME, DATABASE_NAME))
    print('Connection Created')
except odbc.DatabaseError as e:
    print('Database Error:')
    print(str(e.value[1]))
except odbc.Error as e:
    print('Connection Error:')
    print(str(e.value[1]))
else:
    # sql_query = """
    # SELECT TOP 2000 Traffic_Report_Id, Issue_Reported, Published_Date, Issue_Reported
    # FROM Austin_Traffic_Incident
    # """

    sql_query = """
    SELECT Issue_Reported, COUNT(Issue_Reported) as Incident_Count
    FROM Austin_Traffic_Incident
    # WHERE YEAR(Published_DATE) IN (?)
    GROUP BY Issue_Reported
    """
    
    cursor = conn.cursor()
    # cursor.execute(sql_query)
    cursor.execute(sql_query, [2020])

    """
    Step 1.2 Retrieve Dataset from SQL Server
    """
    recordset = cursor.fetchall()

    columns = [col[0] for col in cursor.description]

    df = pd.DataFrame(recordset, columns=columns)

    if 'published_date' in df.columns:
        df['published_date'] = df['published_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    recordset = df.values.tolist()


    """
    Step 2. Export Dataset to Google Spreadsheets
    """
    gs_sheet_id = '<google sheets id>'
    tab_id = '<tab id> INT'
    
    CLIENT_SECRET_FILE = 'client_secret.json'
    API_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

    # create spreadsheets reference object
    mySpreadsheets = service.spreadsheets().get(
        spreadsheetId=gs_sheet_id
    ).execute()

    recordset

    tab_name = [sheet['properties']['title'] for sheet in mySpreadsheets['sheets'] if sheet['properties']['sheetId'] == tab_id][0]
    
    """
    Clear workshete content
    """
    service.spreadsheets().values().clear(
        spreadsheetId=gs_sheet_id,
        range=tab_name
    ).execute()


    """
    Insert dataset
    """
    def construct_request_body(value_array, dimension: str='ROWS') -> dict:
        try:
            request_body = {
                'majorDimension': dimension,
                'values': value_array
            }
            return request_body
        except Exception as e:
            print(e)
            return {}

    """
    Insert column names
    """
    request_body_columns = construct_request_body([columns])
    service.spreadsheets().values().update(
        spreadsheetId=gs_sheet_id,
        valueInputOption='USER_ENTERED',
        range=f'{tab_name}!A1',
        body=request_body_columns
    ).execute()

    """
    Insert rows
    """
    request_body_values = construct_request_body(recordset)
    service.spreadsheets().values().update(
        spreadsheetId=gs_sheet_id,
        valueInputOption='USER_ENTERED',
        range=f'{tab_name}!A2',
        body=request_body_values
    ).execute()

    print('Task is complete')

    cursor.close()
    conn.close()
