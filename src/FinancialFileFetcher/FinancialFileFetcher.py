import datetime as dt
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import lxml

def get_urls(l_identifier, api_key, start_date = None, end_date = None, l_file_form = None):
    """
    Get the urls of financial files.

    Parameters
    ----------
    l_identifier: list[String1, String2, ...]
        a list of company identifier
        e.g. ['AAPL'], ['AAPL', 'MSFT', 'TSLA']

    api_key: String
        user's api key for Last-10K

    start_date: string,'%Y-%m-%d'
        The date that users want to start the search
        e.g.'2020-01-01'
        
    end_date: string,'%Y-%m-%d'
        The date that users want to end the search
        e.g.'2021-12-09'
    
    l_file_form: List[String1, String2, ...]
        a list of required file form
        e.g. ['10-K'], ['10-Q'], ['10-K', '10-Q']

    Returns
    -------
    pd.DataFrame
      A DataFrame containing information (including url) of financial files.
      Columns:
      identifier, Date, formType, name, url, acessionNumber


    Examples
    --------
    >>> import FinancialFileFetcher as FFF
    >>> tmp_df = get_urls(['TSLA'], mykey, start_date = '2020-01-02')
    >>> tmp_df
    identifier  Date    formType    name    url accessionNumber
    0   TSLA    2021-12-01  8-K Material Event Report   https://www.sec.gov/Archives/edgar/data/131860...   0001564590-21-058953
    """
    ret_df = pd.DataFrame()
    
    for identifier in l_identifier:
        headers = {
            # Request headers
            'Ocp-Apim-Subscription-Key': api_key,
        }
        identifier = identifier

        r = requests.get('https://services.last10k.com/v1/company/' + identifier + '/filings', headers = headers)
        #https://services.last10k.com/v1/company/{identifier}/filings
        if r.status_code == 200:
            print(identifier + ' get')
        else:
            print(identifier + ' getting url FAILED!')
            continue
        fs_json = r.json()
        fs_json_result = fs_json['data']['attributes']['result']
        fs_json_df = pd.DataFrame(fs_json_result, dtype = str)
        fs_json_df['identifier'] = identifier
        #print(fs_json_df)
        ret_df = pd.concat([ret_df, fs_json_df], ignore_index=True)
    
    ret_df['Date'] = ret_df['filingDate'].map(lambda x:dt.datetime.strptime(x[:10], '%Y-%m-%d'))
    
    try:
        sd = dt.datetime.strptime(start_date, '%Y-%m-%d')
        ret_df = ret_df[ret_df['Date'] >= sd]
    except:
        pass
    
    try:
        ed = dt.datetime.strptime(end_date, '%Y-%m-%d')
        ret_df = ret_df[ret_df['Date'] >= ed]
    except:
        pass
    
    try:
        ret_df['Type_tag'] = ret_df['formType'].map(lambda x: x in l_file_form)
        ret_df = ret_df[ret_df['Type_tag'] == True].iloc[:,:-1]
    except:
        pass
    
    ret_df = ret_df[['identifier', 'Date', 'formType', 'name', 'url', 'accessionNumber']]
    ret_df = ret_df.reset_index(drop = True)
    return ret_df


def get_latest_files(l_identifier, api_key, number = None, l_file_form = None):
    """
    Get the most updated financial files of given companies.

    Parameters
    ----------
    l_identifier: list[String1, String2, ...]
        a list of company identifier
        e.g. ['AAPL'], ['AAPL', 'MSFT', 'TSLA']

    api_key: String
        user's api key for Last-10K

    number: int
        latest *number* of files
    
    l_file_form: List[String1, String2, ...]
        a list of required file form
        e.g. ['10-K'], ['10-Q'], ['10-K', '10-Q']

    Returns
    -------
    pd.DataFrame
      A DataFrame containing information (including url) of financial files.
      Columns:
      identifier, Date, formType, name, url, acessionNumber


    Examples
    --------
    >>> import FinancialFileFetcher as FFF
    >>> tmp_df = get_latest_files(['AAPL', 'MSFT'], mykey, 2, ['10-K', '10-Q'])
    >>> tmp_df
    identifier  Date    formType    name    url accessionNumber
    0   AAPL    2021-10-29  10-K    Annual Report   https://www.sec.gov/Archives/edgar/data/320193...   0000320193-21-000105
    MSFT    2021-10-26  10-Q    Quarterly Report    https://www.sec.gov/Archives/edgar/data/789019...   0001564590-21-051992
    """
    ret_df = pd.DataFrame()
    
    if l_file_form:
        init_df = get_urls(l_identifier = l_identifier, api_key = api_key, l_file_form = l_file_form)
    else:
        init_df = get_urls(l_identifier = l_identifier, api_key = api_key)
    
    if number:
        ret_df = init_df.sort_values('Date', ascending = False).head(number)
    else:
        ret_df = init_df
    ret_df = ret_df.reset_index(drop = True)
    
    return ret_df


def get_table_info(url, email):
    """
    Fetch names and urls of all tables within a given financial statement

    Parameters
    ----------
    url: String
        financial statement url of a given company and a given season
    
    email: String
        User's Email for this request
        SEC API requires email for identification

    Returns
    -------
    pd.DataFrame
        output names of all the tables contained in the given financial statement and their corresponding urls

    Examples
    --------
    >>> import FinancialFileFetcher as FFF
    >>> tmp_df = get_table_info(url, email)[:5]
    >>> tmp_df
    Table_name  url
    Cover Page  https://www.sec.gov/Archives/edgar/data/320193...
    CONSOLIDATED STATEMENTS OF OPERATIONS   https://www.sec.gov/Archives/edgar/data/320193...
    CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME https://www.sec.gov/Archives/edgar/data/320193...
    CONSOLIDATED BALANCE SHEETS https://www.sec.gov/Archives/edgar/data/320193...
    CONSOLIDATED BALANCE SHEETS (Parenthetical) https://www.sec.gov/Archives/edgar/data/320193...
    """
    #create API request headers
    headers = {'User-Agent': email, 'Accept-Encoding':'gzip', 'Host':'www.sec.gov'}
    
    #edit endpoint
    lst = re.findall(r"\/[^\/]+", url)[-1]
    xml_summary = url.replace(lst,'/FilingSummary.xml')
    
    #request content
    content = requests.get(xml_summary, headers = headers).content
    
    #parse the content
    soup = BeautifulSoup(content, 'lxml')
    reports = soup.find('myreports')
    master_reports = []
    # loop until the last second one (the last report is not what we want)
    for report in reports.find_all('report')[:-1]:
        report_dict = {}
        report_dict['name_short'] = report.shortname.text
        report_dict['url'] = url.replace(lst, '/' + report.htmlfilename.text)

        master_reports.append(report_dict)
        
    df_tables = pd.DataFrame(master_reports)
    df_tables.index = df_tables['name_short']
    df_tables.index.name = 'Table_name'
    df_tables = df_tables.drop('name_short', axis = 1)
    
    return df_tables

#Display Names of All Tables Within a Financial Statement
def display_table_name(url, email):
    """
    Fetch names and urls of all tables within a given financial statement

    Parameters
    ----------
    url: String
        financial statement url of a given company and a given season
    
    email: String
        User's Email for this request
        SEC API requires email for identification

    Returns
    -------
    List
        list of all the tables contained in the given financial statement
    Examples
    --------
    >>> import FinancialFileFetcher as FFF
    >>> display_table_name(url, email)
    ['Cover Page',
     'CONSOLIDATED STATEMENTS OF OPERATIONS',
     'CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME',
     'CONSOLIDATED BALANCE SHEETS',
     'CONSOLIDATED BALANCE SHEETS (Parenthetical)',
     "CONSOLIDATED STATEMENTS OF SHAREHOLDERS' EQUITY",
     'CONSOLIDATED STATEMENTS OF CASH FLOWS',
     'Summary of Significant Accounting Policies',
     'Revenue Recognition',
     'Financial Instruments',
     ...]
    """
    table_info = get_table_info(url, email)
    table_name = table_info.index.to_list()
    return table_name




#Fetch the url with the name of a financial table
def get_table_url(fs_url, email, table_name):
    """
    Fetch names and urls of all tables within a given financial statement

    Parameters
    ----------
    url: String
        financial statement url of a given company and a given season
    
    email: String
        User's Email for this request
        SEC API requires email for identification

    table_name: String
        Exact table name to request its url

    Returns
    -------
    String
        url of the financial table
    Examples
    --------
    >>> import FinancialFileFetcher as FFF
    >>> get_table_url(url, email, 'CONSOLIDATED STATEMENTS OF CASH FLOWS')
    'https://www.sec.gov/Archives/edgar/data/320193/000032019321000105/R7.htm'
    """
    table_info = get_table_info(fs_url, email)
    try:
        ret_url = table_info[table_info.index == table_name]['url'].values[0]
        return ret_url
    except:
        print('ERROR! Please double check the input table name!')
        return None

def display_table(fs_url, table_name, email):
    """
    Scrape tables from financial files

    Parameters
    ----------
    fs_url: String
        financial statement url

    table_name: String
        Exact table name to request its url

    email: String
        User's Email for this request
        SEC API requires email for identification

    Returns
    -------
    DataFrame
        financial table

    Examples
    --------
    >>> import FinancialFileFetcher as FFF
    >>> tmp_df = display_table(url, 'CONSOLIDATED STATEMENTS OF OPERATIONS', email)
    >>> tmp_df
    https://www.sec.gov/Archives/edgar/data/320193/000032019321000105/R2.htm
    Requested the web content...
    Parsed the table...
    Stored the data and sections into 2 DataFrames...
    Converted string into digit...
    Merged 2 DataFrames...
    Renamed indices and columns...
    SUCCESS!
    Sep. 25, 2021   Sep. 26, 2020   Sep. 28, 2019
    CONSOLIDATED STATEMENTS OF OPERATIONS - USD ( )𝑠ℎ𝑎𝑟𝑒𝑠𝑖𝑛𝑇ℎ𝑜𝑢𝑠𝑎𝑛𝑑𝑠,  in Millions - 12 Months Ended            
    Net sales   365817.0    274515.0    260174.0
    Cost of sales   212981.0    169559.0    161782.0
    Gross margin    152836.0    104956.0    98392.0
    Operating expenses:         
    Research and development    21914.0 18752.0 16217.0
    Selling, general and administrative 21973.0 19916.0 18245.0
    Total operating expenses    43887.0 38668.0 34462.0
    Operating income    108949.0    66288.0 63930.0
    Other income/(expense), net 258.0   803.0   1807.0
    Income before provision for income taxes    109207.0    67091.0 65737.0
    Provision for income taxes  14527.0 9680.0  10481.0
    Net income  94680.0 57411.0 55256.0
    ...
    """
    table_url = get_table_url(fs_url, email, table_name)
    print(table_url)# print the url for analyst to check the digit or the unit
    headers = {'User-Agent': email, 'Accept-Encoding':'gzip', 'Host':'www.sec.gov'}

    statement_data = {}
    statement_data['headers'] = []
    statement_data['sections'] = {}
    statement_data['data'] = {}

    try:
        content = requests.get(table_url, headers = headers).content
        report_soup = BeautifulSoup(content, 'html')
        print('Requested the web content...')
    except:
        print('ERROR!!!We encountered an error when requesting the web content!')
        return None

    for index, row in enumerate(report_soup.table.find_all('tr')):

        # first let's get all the elements.
        cols = row.find_all('td')

        # if it's a regular row and not a section or a table header
        if (len(row.find_all('th')) == 0 and len(row.find_all('strong')) == 0): 
            reg_row = [ele.text.strip() for ele in cols]
            statement_data['data'][index] = reg_row

        # if it's a regular row and a section but not a table header
        elif (len(row.find_all('th')) == 0 and len(row.find_all('strong')) != 0):
            #sec_row = cols[0].text.strip()
            sec_row = [ele.text.strip() for ele in cols]
            statement_data['sections'][index] = sec_row

        # finally if it's not any of those it must be a header
        elif (len(row.find_all('th')) != 0):            
            hed_row = [ele.text.strip() for ele in row.find_all('th')]
            statement_data['headers'].append(hed_row)

        else:            
            print('ERROR!!!We encountered an error in parsing the table!')
            return None

    print('Parsed the table...')

    try:
        df = pd.DataFrame(statement_data['data']).T
        sections_df = pd.DataFrame(statement_data['sections']).T
        print('Stored the data and sections into 2 DataFrames...')
    except:
        print('ERROR!!!We encountered an error in storing the data and sections into 2 DataFrames!')
        return None
    
    try:
        # Get rid of the '$', '(', ')', and convert the '' to NaNs.
        df.iloc[:,1:] = df.iloc[:,1:].replace('[\$,)]','', regex=True )\
                                 .replace( '[(]','-', regex=True)\
                                 .replace( '', 'NaN', regex=True)
        # everything is a string, so let's convert all the data to a float.
        df.iloc[:,1:] = df.iloc[:,1:].astype(float)   
        print('Converted string into digit...')
    except:
        print('ERROR!!!We encountered an error in converting string into digit!')
        return df #return the half-processed df
    
    try:
        ret_df = pd.concat([df, sections_df])
        ret_df = ret_df.sort_index()
        print('Merged 2 DataFrames...')
    except:
        print('ERROR!!!We encountered an error in merging 2 DataFrames!')
        return df #return the half-processed df
    
    try:
        ret_df.index = ret_df[0]
        ret_df = ret_df.drop(0, axis = 1)
        ret_df.index.name = (' - ').join(statement_data['headers'][0]) 

        # Change the column headers
        len_header = len(statement_data['headers'])
        if len_header == 2:
            header =  statement_data['headers'][1]
        elif len_header == 1:
            header = statement_data['headers'][0][1:]
        else:
            print('We encountered an error in framing the header')
            return None
        ret_df.columns = header
        print('Renamed indices and columns...')
    except:
        print('ERROR!!!We encountered an error in renaming indices and columns!')
        return ret_df #return the half-processed ret_df
    
    print('SUCCESS!')
    return ret_df

