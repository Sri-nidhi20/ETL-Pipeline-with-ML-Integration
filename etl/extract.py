#this module is responsible for:
#read the csv file and return it as a pandas dataframe
import pandas as pd
def extract_data(filepath):
    """
    Reads a csv file from the given filepath.
    returns a pandas dataframe (a table of data).
    filepath = the location of the csv file
       example: "data/sample_data.csv"
    """
    print(f"[EXTRACT] Reading file: {filepath}")
    
    df = pd.read_csv(filepath)
    
    print(f"[EXTRACT] Rows loaded: {df.shape[0]}, Columns: {df.shape[1]}")

    return df