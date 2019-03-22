from mysqlWrapper.mariadb import MariaDb
from conf.config import get_config
from HashTagExtractor import get_data

ADD_HASHTAG = ("INSERT INTO hashtag (name) VALUES (%s)")


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
print(unique_hashtags)

#TODO wtf
counter = 1
for uni in unique_hashtags:
    print(uni)
    connector.execute_ex(ADD_HASHTAG,(uni))