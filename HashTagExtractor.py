import datetime
import pandas as pd
import re

def fetch_data(path):
    categories = [
        "PRODUCT_ID",
        "TITLE",
        "PLANNING_WEEK",
        "ASSORTMENT_CATEGORY1",
        "ASSORTMENT_CATEGORY2",
        "ASSORTMENT_CATEGORY3",
        "ASSORTMENT_CATEGORY4"
        ]
    df = pd.read_excel(path, sheet_name=0, encoding='windows-1252')
    return df[categories].dropna(subset=["PLANNING_WEEK"])

def filter_by_week(data_frame,planning_week):
    return data_frame[data_frame["PLANNING_WEEK"] == planning_week]


def extract_hashtags(data_frame):
    data_frame["HASHTAGS"] = data_frame.apply(lambda row: get_hashtag(row), axis=1)
    return data_frame


def get_hashtag(row):
    #TODO make more sophisticated
    hashtag = "UKNOWN"
    potential_rows =["ASSORTMENT_CATEGORY4","ASSORTMENT_CATEGORY3","ASSORTMENT_CATEGORY2","ASSORTMENT_CATEGORY1"]
    for category in potential_rows:
        if is_valid_hashtag(row[category]):
            hashtag = row[category]
            break;
    hashtag = hashtag.strip()
    # split words  if the have a binding sign
    hashtag = hashtag.replace("-"," ").replace("&", " ")
    #remove multiplie spaces with one
    hashtag = ' '.join(hashtag.split())
    # add hashtags in between and at the beginning
    hashtag = "#" + hashtag.replace(" ","#")


    return hashtag

def is_valid_hashtag(hashtag):
    if isinstance(hashtag, str) is False:
        return False
    if "." in hashtag:
        return False
    if "/" in hashtag:
        return False
    if "," in hashtag:
        return False
    return True

print("Starting HashtagExtractor")
raw_data = fetch_data("all-products_short.xlsx")
filtered_data = filter_by_week(raw_data,"2019-W11")
result = extract_hashtags(filtered_data)
print(result.head())

