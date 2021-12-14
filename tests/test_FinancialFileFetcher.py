from FinancialFileFetcher import FinancialFileFetcher

def test_geturls():
    l_identifier = ['AAPL', 'MSFT']
    test_key = 'a42ff36922e94fbba2a2ad35a595905f'
    start_date = '2020-01-02'
    end_date = '2021-09-01'
    l_file_form = ['10-Q','10-K']
    ret_df = FinancialFileFetcher.get_urls(l_identifier = l_identifier, api_key = 'a42ff36922e94fbba2a2ad35a595905f', start_date = start_date, end_date = end_date, l_file_form = l_file_form)
    assert ret_df.shape[1] == 6, "Should output a DataFrame with 6 columns"