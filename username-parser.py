from inscrawler import InsCrawler
from mysqlWrapper.mariadb import MariaDb
from conf.config import get_config
import sys
import json
import re

config = get_config("MARIADBX")
connector = MariaDb(db="db")
crawler = InsCrawler(has_screen=False)


ADD_USER = ("INSERT INTO user (username, hashtag, img,post_num,follower_num,following_num) VALUES (%s, %s,%s,%s, %s,%s)")

def open_tag_data(file):
    with open(file, 'r', encoding='utf8') as f:
        raw = f.readlines()
        raw = json.dumps(raw)
        return json.loads(raw)

def process_raw_data(raw):
    result = list()
    raw = raw[0][1: -1]
    matches = re.findall(r'\[(.*?)\]',raw)
    for match in matches:
        tup  = match.split(",")
        result.append((tup[0].strip().replace('"',""), tup[1].strip().replace('"',"")))
    return result


raw = open_tag_data("instaparse.json")
tuples = process_raw_data(raw)
for dataset in tuples:
    profile_data =crawler.get_data_from_post(dataset[1])
    data =  (
            profile_data["name"],
            "#"+dataset[0],
            profile_data["photo_url"],
            profile_data["post_num"],
            profile_data["follower_num"],
            profile_data["following_num"]
        )
    try:
        connector.execute_ex(ADD_USER,data)
    except e:
        print(e)
        pass
    break

sys.exit()

