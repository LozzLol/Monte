import sqlite3
import json
from datetime import datetime

timeframe = "2006-09"
# Used to hold the transaction so we can carry out all queries at once
sql_transaction = []
# Connects to the database
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()


def create_table():
    c.execute("""CREATE TABLE IF NOT EXISTS parent_reply
    (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, 
    comment TEXT, subreddit TEXT, unix INT, score INT)""")


def find_parent(pid):
    # Finds the parent body of text
    sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
    c.execute(sql)
    result = c.fetchone()
    if result is not None:
        return result[0]
    else:
        return False
    # except Exception as e:
    # print("find_parent", e)
    # return False


def format_data(data):
    # Sanitise some of the data
    data = data.replace("\n", " newlinechar ").replace("\r", " newlinechar ").replace('"', "'")
    return data


# Create table if it doesnt exist
if __name__ == "__main__":
    create_table()
    row_counter = 0
    # how many relationships have we come up with
    paired_rows = 0
    # Iterate through the file
    with open("D:/Documents/reddit_comments/RC_{}".format(timeframe), buffering=1000) as f:
        for row in f:
            print(row)
            row_counter +=1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']

            parent_data = find_parent(parent_id)
