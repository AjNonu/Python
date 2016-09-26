import urllib3
import ast
import mysql.connector
import time

token = ""

url = "https://graph.facebook.com/508246595873195/posts?limit=1&access_token=%s" % token
user_agent = 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.63 Safari/534.3'
headers = {'User-Agent': user_agent}


def get_likes_count(val):
    print("In Likes")
    urllike = "https://graph.facebook.com/%s?fields=likes.limit(0).summary(true)&access_token=%s" % (val, token)
    req = urllib3.Request(urllike, None, headers)
    req2 = urllib3.urlopen(req)
    page = req2.read()
    page = str(page)
    data = ast.literal_eval(page)
    total_count_like = data['likes']['summary']['total_count']
    return total_count_like


def get_comments_count(val):
    print("In Comments")
    urllike = "https://graph.facebook.com/%s?fields=comments.limit(0).summary(true)&access_token=%s" % (val,token)
    req = urllib3.Request(urllike, None, headers)
    req2 = urllib3.urlopen(req)
    page = req2.read()
    page = str(page)
    data = ast.literal_eval(page)
    total_count_comment = data['comments']['summary']['total_count']
    return total_count_comment


def save_likes_data(obj_id, next_url):
    print ("In Save Likes Data")
    urllike = "https://graph.facebook.com/%s?fields=likes.limit(1000)&access_token=%s" % (obj_id,token)
    while True:
        req = urllib3.Request(urllike, None, headers)
        req2 = urllib3.urlopen(req)
        page = req2.read()
        page = str(page)
        data = ast.literal_eval(page)
        try:
            base = data['likes']['data']
        except KeyError:
            base = data['data']
        length = len(base)
        for num in xrange(length):
            user_id = base[num]['id']
            user_name = base[num]['name']
            if "'" in user_name:
                user_name = str(user_name)
                user_name = user_name.replace("'", " ")
            cnx = mysql.connector.connect(host='127.0.0.1', database='database', user='root', password='******')
            cursor = cnx.cursor()
            # sql Query
            query = ("insert INTO TableName(objectID, User_ID, User_Name)  VALUES('" + obj_id + "', '" + user_id + "', '" + user_name + "');")
            try:
                cursor.execute(query)
            except:
                print ("Error here %s : %s") % (user_name,user_id)
            cnx.commit()
            cursor.close()
            cnx.close()
        try:
            urllike = data['likes']['paging']['next']
        except KeyError:
            try:
                urllike = data['paging']['next']
            except:
                save_comments_data(obj_id, next_url)


def save_comments_data(obj_id, next_url):
    print ("In Save Comments Data")
    urllike = "https://graph.facebook.com/%s?fields=comments.limit(1000)&access_token=%s" % (obj_id,token)
    while True:
        req = urllib3.Request(urllike, None, headers)
        req2 = urllib3.urlopen(req)
        page = req2.read()
        page = str(page)
        page = page.replace("false", "False")
        data = ast.literal_eval(page)
        try:
            base = data['comments']['data']
        except KeyError:
            base = data['data']
        length = len(base)
        for num in xrange(length):
            user_id = base[num]['from']['id']
            user_name = base[num]['from']['name']
            message = base[num]['message']
            created_time = base[num]['created_time']
            if "'" in message:
                message = str(message)
                message = message.replace("'", " ")
            if "'" in user_name:
                user_name = str(user_name)
                user_name = user_name.replace("'", " ")
            cnx = mysql.connector.connect(host='127.0.0.1', database='DB', user='root', password='******')
            cursor = cnx.cursor()
            # sql Query
            query = ("insert INTO commentdetails(objectID, user_id, user_name, message, created_time)   VALUES('" + obj_id + "', '" + user_id + "', '" + user_name + "','" + message + "','" + created_time + "');")
            try:
                cursor.execute(query)
            except:
                print ("Error here %s : %s") % (user_name,user_id)
            cnx.commit()
            cursor.close()
            cnx.close()
        print ("heeee")
        try:
            urllike = data['comments']['paging']['next']
        except KeyError:
            try:
                urllike = data['paging']['next']
            except:
                get_post(next_url)


def save_to_db(val, next_url):
    print ("SAve to DB")
    page_id = str(val['data'][0]['from']['id']) #PageID
    obj_id = str(val['data'][0]['object_id'])
    try:
        source = str(val['data'][0]['source'])
    except:
        source = "NA"
    message = str(val['data'][0]['message'])
    post_id = str(val['data'][0]['id']) # Post ID
    post_type = str(val['data'][0]['type'])
    created_time = str(val['data'][0]['created_time'])
    picture = str(val['data'][0]['picture'])
    like_count = str(get_likes_count(obj_id))
    comment_count = str(get_comments_count(obj_id))
    share_count = str(val['data'][0]['shares']['count'])
    # Save To DataBase
    # connect to database
    cnx = mysql.connector.connect(host='127.0.0.1', database='Db', user='root', password='******')
    cursor = cnx.cursor()
    # sql Query
    query = (
    "insert INTO posts(pageID, objectID, message, postID, postTYPE, totalLIKES, totalCOMMENTS, totalSHARES, picture, source, createdTIME) VALUES('" + page_id + "','" + obj_id + "','" + message + "','" + post_id + "','" + post_type + "','" + like_count + "','" + comment_count + "','" + share_count + "','" + picture + "','" + source + "','" + created_time + "');")
    cursor.execute(query)
    cnx.commit()
    cursor.close()
    cnx.close()
    save_likes_data(obj_id, next_url)


def get_post(url):
    print ("IN GETPOST")
    req = urllib3.Request(url, None, headers)
    req2 = urllib3.urlopen(req)
    page = req2.read()
    page = str(page)
    page = page.replace("false", "False")
    data = ast.literal_eval(page)
    next_url = get_next_url(data)
    save_to_db(data, next_url)


def get_next_url(data):
    print ("IN Get Next URL Function")
    next_url = data['paging']['next']
    return next_url

get_post(url)

print ("this is end")
