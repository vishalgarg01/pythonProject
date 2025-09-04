import logging
import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient


def convert_to_epoc(from_date):
    date_object = datetime.strptime(str(from_date), '%Y-%m-%d %H:%M:%S')
    res = int(date_object.timestamp()) * 1000
    logging.info(res)
    return res


def remove_records(collection, from_date, to_date):
    max_delete_count = 400000
    batch_size = 1000
    logging.info("removing records in %s collection from %s to %s", collection, from_date, to_date)
    to_datetime = datetime.combine(to_date, datetime.min.time())
    tmp_datetime = datetime.combine(from_date, datetime.min.time())
    count = 0
    while tmp_datetime < to_datetime:
        tmp_datetime_2 = tmp_datetime + timedelta(days=1)
        start_epoch = convert_to_epoc(tmp_datetime)
        end_epoch = convert_to_epoc(tmp_datetime_2)
        inner_count = 0  # Reset the count for each day
        while tmp_datetime < tmp_datetime_2:
            removed_result_deleted_count = 0
            query = {"eventStartTimestamp": {"$gte": start_epoch, "$lt": end_epoch}}
            documents_to_delete = collection.find(query).limit(batch_size)
            document_ids = [doc['_id'] for doc in documents_to_delete]
            removed_result = collection.delete_many({"_id": {"$in": document_ids}})
            inner_count += removed_result.deleted_count
            removed_result_deleted_count = removed_result.deleted_count
            # query = {"eventStartTimestamp": {"$gte": start_epoch, "$lt": end_epoch}}
            # removed_result = collection.delete_many(query, limit=batch_size)

            logging.info("removed  %s inner records from %s to %s", removed_result_deleted_count, tmp_datetime,
                         tmp_datetime_2)
            # Check if there are no more records left for this day
            if removed_result_deleted_count == 0 or inner_count >= max_delete_count:
                logging.info("innerloop removed_result_count - %s. inner_count - %s.", removed_result_deleted_count,
                             inner_count)
                break
        logging.info("Processed records for %s to %s. Total removed: %s", tmp_datetime, tmp_datetime_2, inner_count)
        count += inner_count
        if count >= max_delete_count:
            logging.info("Reached the maximum delete count of %s. Breaking the outer loop.", max_delete_count)
            break
        tmp_datetime = tmp_datetime_2
    logging.info("removed total records - %s", count)


logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
today = datetime.today().date()
logging.info("today - %s", today)
three_months_before_date = today - timedelta(days=90)
logging.info("three months before - %s", three_months_before_date)

username = os.environ['EVENT_FOREST_DB_MONGO_USERNAME']
password = os.environ['EVENT_FOREST_DB_MONGO_PASSWORD']
host = os.environ['EVENT_FOREST_DB_MONGO_HOST']
client = MongoClient(host, username=username, password=password, authSource='admin')
eventForest_db = client['eventforest']
eventLog_collection = eventForest_db['eventLog']

min_date = eventLog_collection.aggregate([{"$group": {"_id": None, "min": {"$min": "$eventStartTimestamp"}}}])
epoc_date = [i for i in min_date][0]['min']
min_date = datetime.fromtimestamp(epoc_date / 1000).date()
logging.info("min date in events log collection %s", min_date)
remove_records(eventLog_collection, min_date, three_months_before_date)
client.close()