# create a function to take all of the column in a dataframe and normalise them
def clean_columns(dataframe):
    dataframe = dataframe.apply(lambda x: x.str.strip(), axix=0)
    dataframe = dataframe.apply(lambda x: x.str.lower)
    return dataframe
