from connection import *

def get_topics_from_netezza(timestamp):

    topics = idadb.ida_query(f'SELECT topics from topics where timestamp>={timestamp};')
    return topics