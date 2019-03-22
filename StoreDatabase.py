from mysqlWrapper.mariadb import MariaDb
from conf.config import get_config
from HashTagExtractor import get_data

ADD_HASHTAG = ("INSERT INTO hashtag (name) VALUES (%s) ON DUPLICATE KEY UPDATE name = VALUES(name)")
ADD_PRODUCT = ("INSERT INTO product (title, img) VALUES (%s, %s)")
ADD_PRODUCT_HASHTAG = ("INSERT INTO product_hashtag (product_id, hashtag_id) VALUES (%s, %s)")


config = get_config("MARIADBX")
connector = MariaDb(db="db")

df = get_data("2019-W11")

hashtags = df["HASHTAGS"]

unique_hashtags = set()
for value in hashtags:
    separated_values = value.split("#")
    for token in separated_values:
        if len(token) == 0:
            continue
        unique_hashtags.add("#"+token)


for uni in unique_hashtags:
    try:
        connector.execute_ex(ADD_HASHTAG,(uni))
        print("{} added".format(uni))
    except e:
        print("{} could not be added {}".format(uni, e))

for index,row in df.iterrows():
    data = (row["TITLE"] or "", row["MAIN_IMAGE_URL"] or "")
    try:
        print(data)
        product_id = connector.execute_ex(ADD_PRODUCT, data)
        print("{} product added".format(row["PRODUCT_ID"]))
        for uni in unique_hashtags:
            if uni in row["HASHTAGS"]:
                try:
                    connector.execute_ex(ADD_PRODUCT_HASHTAG, (product_id, uni))
                except e:
                    print(e)
    except e:
        print("{} product could not be added {}".format(row["TITLE"] or "",e))

