import datetime
import pandas as pd

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
    data_frame["HASHTAGS"] = data_frame.apply(tmp)


def tmp(x,y):
    print(x,y)
    return "x"

print("Starting HashtagExtractor")
raw_data = fetch_data("all-products_short.xlsx")
filtered_data = filter_by_week(raw_data,"2019-W11")
extract_hashtags(filtered_data)
print(filtered_data.head())

