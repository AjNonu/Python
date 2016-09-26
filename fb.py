# -*- coding: utf-8 -*-

import MySQLdb
import ast
import re
import requests

from textblob import TextBlob
from textblob.np_extractors import ConllExtractor
from textblob.taggers import NLTKTagger

extractor = ConllExtractor()
nltk_tagger = NLTKTagger()

# Access Token
token = ""

page_id = ""  # Direct Axis


name = "DirectAxis"

# Connect to database


def dbconnection():
    global conn
    global cursor
    conn = MySQLdb.connect("sqldb", "username", "password",
                           "dbname", use_unicode=True, charset="utf8")
    cursor = conn.cursor()


dbconnection()


def getCompanyLogo():
    logoURL = "https://graph.facebook.com/%s?fields=picture.width(100).height(100)&access_token=%s" % (
        page_id, token)
    page = requests.get(logoURL)
    page = str(page.json())
    page = page.replace("false", "False")
    page = page.replace("true", "True")
    data = ast.literal_eval(page)
    logo = data['picture']['data']['url']
    # Save to Database
    query = "update AA_Company SET logo=%s where posts_fb=%s;" % (logo, name)
    cursor.execute(query)
    conn.commit()


try:
    getCompanyLogo()
except:
    print("error getting logo")
    exit()


class Fdata:
    def __init__(self):
        self.token = ""

    def save_comments_data(self, obj_id_original):
        print("saving comments")
        url_comments = "https://graph.facebook.com/%s?fields=comments.limit(1000),object_id&access_token=%s" % (
            obj_id_original, self.token)
        obj_id = obj_id_original.split("_")[1]
        while True:
            page = requests.get(url_comments)
            page = str(page.json())
            page = page.replace("false", "False")
            data = ast.literal_eval(page)
            try:
                base = data['comments']['data']
            except KeyError:
                try:
                    base = data['data']
                except:
                    return
            length = len(base)
            for num in range(length):
                user_id = base[num]['from']['id']
                user_name = base[num]['from']['name']
                message = base[num]['message']
                comment_object_id = base[num]['id']
                all_words = []
                tagged = TextBlob(message, pos_tagger=nltk_tagger)
                pos = tagged.pos_tags
                for w in pos:
                    all_words.append(w[0])

                all_words = set(all_words)
                all_words = list(all_words)
                pol = []
                for word in all_words:
                    text = TextBlob(word)
                    text.correct()
                    if text.polarity != 0:
                        pol.append(text.polarity)
                sum = 0
                for n in pol:
                    sum = sum + n
                try:
                    sentiment = str(round(sum / len(pol), 2))
                except:
                    sentiment = ("0")
                # Sentiment Part 2 Ends

                created_time = base[num]['created_time']
                created_time = created_time.split('T')[0]
                if "'" in message:
                    message = str(message)
                    message = message.replace("'", " ")
                if "'" in user_name:
                    user_name = str(user_name)
                    user_name = user_name.replace("'", " ")
                # Save To Database
                query = "insert INTO commentdetails_fb(objectID, user_id, user_name, comment_ID, message, created_time, sent_pol)   VALUES('" + obj_id + "', '" + user_id + "', '" + user_name + "','" + comment_object_id + "','" + message + "','" + created_time + "', '" + sentiment + "');"
                try:
                    cursor.execute(query)
                    conn.commit()
                except:
                    continue
                    # print("Error Here %s : %s" % (user_name,user_id))
            try:
                url_comments = data['comments']['paging']['next']
            except KeyError:
                try:
                    url_comments = data['paging']['next']
                except:
                    return

    def save_to_db(self, page_data, page_id_id):
        # ********** #
        try:
            for i in range(100):
                obj_id_original = str(page_data['data'][i]['id'])
                obj_id = obj_id_original.split("_")[1]
                q_mysql = "SELECT objectID FROM posts_fb where objectID='%s';" % obj_id
                cursor.execute(q_mysql)
                for objid in cursor:
                    q_mysql = objid[0]
                    break
                if int(obj_id) == q_mysql:
                    return
                # Object ID
                obj_id_original = str(page_data['data'][i]['id'])
                obj_id = obj_id_original.split("_")[1]
                try:
                    source = str(page_data['data'][i]['source'])
                except:
                    source = "NA"
                try:
                    message = str(page_data['data'][i]['message'])
                except:
                    message = " "
                all_words = []
                tagged = TextBlob(message, pos_tagger=nltk_tagger)
                pos = tagged.pos_tags
                for w in pos:
                    all_words.append(w[0])

                all_words = set(all_words)
                all_words = list(all_words)
                pol = []

                for word in all_words:
                    text = TextBlob(word)
                    if text.polarity != 0:
                        pol.append(text.polarity)
                # Sentiment Part 2 Ends
                sum = 0
                for n in pol:
                    sum = sum + n
                try:
                    sentiment = str(round(sum / len(pol), 2))
                except:
                    sentiment = ("0")
                post_id = str(page_data['data'][i]['id'])  # Post ID
                post_type = str(page_data['data'][i]['type'])
                created_time = str(page_data['data'][i]['created_time'])
                created_time = created_time.split('T')[0]
                try:
                    picture = str(page_data['data'][i]['picture'])
                except:
                    picture = "NA"
                like_count = str(page_data['data'][i]['likes']['summary']['total_count'])
                comment_count = str(page_data['data'][i]['comments']['summary']['total_count'])
                try:
                    share_count = str(page_data['data'][i]['shares']['count'])
                except:
                    share_count = str(0)
                # Check DB for Article
                result = ""
                q_mysql = "SELECT postID FROM posts_fb where postID='%s';" % post_id
                cursor.execute(q_mysql)
                for i in cursor:
                    result = result + i[0]
                if result:
                    print("Updated Successfully")
                    exit()
                # Save To Database
                query = "insert INTO posts_fb(name, pageID, objectID, message, postID, postTYPE, totalLIKES, totalCOMMENTS, totalSHARES, picture, source, createdTIME, sent_pol) VALUES('" + name + "','" + page_id_id + "','" + obj_id + "','" + message + "','" + post_id + "','" + post_type + "','" + like_count + "','" + comment_count + "','" + share_count + "','" + picture + "','" + source + "','" + created_time + "', '" + sentiment + "');"
                try:
                    cursor.execute(query)
                except:
                    message = message
                    message = re.sub(r'[^a-zA-Z0-9 ]',r'',message)
                    query = "insert INTO posts_fb(name, pageID, objectID, message, postID, postTYPE, totalLIKES, totalCOMMENTS, totalSHARES, picture, source, createdTIME, sent_pol) VALUES('" + name + "','" + page_id_id + "','" + obj_id + "','" + message + "','" + post_id + "','" + post_type + "','" + like_count + "','" + comment_count + "','" + share_count + "','" + picture + "','" + source + "','" + created_time + "','" + sentiment + "');"
                    cursor.execute(query)
                conn.commit()
                self.save_comments_data(obj_id_original)
        except IndexError:
            print("Post Entry Done")

instance = Fdata()  # Unilever


# Facebook graph api URL
url = "https://graph.facebook.com/%s/posts?limit=100&fields=name,object_id,message,id,type,picture,source,created_time,shares,likes.limit(0).summary(true),comments.limit(0).summary(true)&access_token=%s" % (
        page_id, token)


def get_post(url, page_id):
    while True:
        page = requests.get(url)
        page = str(page.json())
        page = page.replace("false", "False")
        page = page.replace("true", "True")
        data = ast.literal_eval(page)
        try:
            instance.save_to_db(data, page_id)
        except:
            print("\n\n\n\n\n\nError Ignored For Testing\n\n\n\n\n\n\n")
        try:
            url = data['paging']['next']
        except:
            print("Program Completed ")
            conn.close()
            cursor.close()
            exit()

get_post(url, page_id)
