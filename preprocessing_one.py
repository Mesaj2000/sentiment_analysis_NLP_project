import pandas as pd
import json
from tqdm import tqdm
import gzip

# The review data fields to include
# All other fields are dropped when data is read
DESIRED_FIELDS = ("reviewText", "vote")


"""
read_data_from_json()
Basic preprocessing function
Kinda garbage rn ngl

Input:    
    path:   A string containing the path to a JSON file storing review data
Output:
    df:     A pandas dataframe containing the review data; only DESIRED_FIELDS are included

TODO (In this function or another):
    Normalize the votes based on product ID (field name "asin")
        Strategies to do this: min-max, softmax, z-score, top quintile binary, rank / num reviews
    Figure out a way to handle 20+ gigs of data efficiently

Things to consider:
By default, the data comes in a .gzip format
I unzipped it to a .json text file, which trippled the file size
.gzip files can be read directly by python without unzipping, but 
this may (or may not idk) make it run slower
We may want to do that
"""
def read_data_from_json(path: str):
    dicts = []

    with open(path) as f:
        for line in tqdm(f):
            d = json.loads(line)
            
            d.setdefault("vote", 0)
            d.setdefault("reviewText", "")
            d.setdefault("summary", "")

            d["reviewText"] = f'{d["summary"]} {d["reviewText"]}'
            
            d = {k:d[k] for k in DESIRED_FIELDS}
            dicts.append(d)

    df = pd.DataFrame(dicts)
    df.to_csv(f"{path[:-5]}_trimmed.csv")


"""
From dataset page
"""
def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)

"""
From dataset page
Works pretty well as-is for small datasets, like "Tools and Home Improvement"
"""
def get_df(path):
    df = dict()
    for i, d in enumerate(parse(path)):
        df[i] = d

    return pd.DataFrame.from_dict(df, orient='index')


if __name__ == "__main__":
    #read_data_from_json("./data/Books_5.json")
    df = get_df("./data/Tools_and_Home_Improvement_5.json.gz")
    print(df)