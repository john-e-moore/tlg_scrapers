"""
    #df = fetch_table(url, 5)
    # Clean spacing and non-ASCII characters
    df = df.applymap(clean_text)
    df.columns = [clean_text(x) for x in df.columns]
    # Trim unnecessary rows
    idx = df[df['Item'] == 'Total Liabilities'].index[0]
    df = df.iloc[1:idx+1,:]
    df.to_csv('balance_sheet.csv')
    print(df.head())
    print(df.shape)
    print(df.dtypes)
    """