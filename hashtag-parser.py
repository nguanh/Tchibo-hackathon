from inscrawler import InsCrawler
from mysqlWrapper.mariadb import MariaDb
from conf.config import get_config
import sys
import json

config = get_config("MARIADBX")
connector = MariaDb(db="db")


GET_HASHTAG = ("SELECT * FROM hashtag")
crawler = InsCrawler(has_screen=False)
NUMBER_OF_IMAGES_PER_TAG = 5;

posts = list()
connector.execute_ex(GET_HASHTAG)

for row in connector.cursor:
    hashtag = row[0].replace("#","")
    crawl_result = crawler.get_latest_posts_by_tag(hashtag,NUMBER_OF_IMAGES_PER_TAG)
    for dataset in crawl_result:
        post_id = dataset["key"].replace("https://www.instagram.com/p","").replace("/","")
        post = (hashtag, post_id)
        #print("crawler {}".format(post))
        posts.append(post)

out = json.dumps(posts, ensure_ascii=False)
with open("instaparse.json", 'w', encoding='utf8') as f:
            f.write(out)
print("insta parse json dump complete")
sys.exit()

