from inscrawler import InsCrawler
from mysqlWrapper.mariadb import MariaDb
from conf.config import get_config
import sys
import json
import re

config = get_config("MARIADBX")
connector = MariaDb(db="db")
crawler = InsCrawler(has_screen=False)


ADD_USER = ("INSERT IGNORE INTO user (username, hashtag, img,post_num,follower_num,following_num) VALUES (%s, %s,%s,%s, %s,%s)")
ADD_POST = ("INSERT INTO posts(id, img, username) VALUES (%s, %s,%s)")

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
    data =crawler.get_data_from_post(dataset[1],3)
    profile_data = data["user"]
    post_list = data["posts"]

    data =  (
            profile_data["name"],
            "#"+dataset[0],
            profile_data["photo_url"],
            profile_data["post_num"],
            profile_data["follower_num"],
            profile_data["following_num"]
        )
    try:
        user_id = connector.execute_ex(ADD_USER,data)
        for post in post_list:
            key_name = post["key"].replace("https://www.instagram.com/p","").replace("/","")
            post_tuple = (key_name,post["img_url"], profile_data["name"])
            connector.execute_ex(ADD_POST, post_tuple)
    except Exception as e:
        print(e)

sys.exit()

