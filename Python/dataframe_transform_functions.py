# create a function to take all of the column in a dataframe and normalise them
def clean_columns(dataframe):
    dataframe.columns = dataframe.columns.str.lower()
    dataframe.columns = dataframe.columns.str.replace(r'[^\w\s]', '')
    dataframe.columns = dataframe.columns.str.replace(' ','_')
    return dataframe
