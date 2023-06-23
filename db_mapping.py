from pymongo import MongoClient
from connect_db import connect_to_mongodb_src
import time

# connection
database_name = "Apply_Filter"
db=connect_to_mongodb_src(database_name)

#collections
src_name = "src_listing"
dst_name = "dst_listing"
connection_to_src_filter = db["src_final_filtered"]
connection_to_dst = db["dst_listing"]
src_in_dst_mapping = db["src_in_dst_mapping"]

def main():
    src_in_dst_mapping.drop()

    src_results = createFileList()
    i=0
    start_time = time.time()
    for each in src_results:
        query = {
                'filename': each['filename']
            }
        results = connection_to_dst.find(query)
        documents=[]
        for result in results:
            doc = {
                'src_id': each['_id'],
                'src_filename': each['filename'],
                'src_filepath': each['filepath'],
                'src_filesize': each['filesize'],
                'dst_id': result['_id'],
                'dst_filename': result['filename'],
                'dst_filepath': result['filepath'],
                'dst_filesize': result['filesize']
            }
            documents.append(doc)
        if len(documents) != 0:
            src_in_dst_mapping.insert_many(documents)
        i=i+1
        if i % 1000 == 0:
            final_time= time.time()
            print(final_time-start_time)
            start_time = time.time()



        


def createFileList():
    aggregation = [
    {
        '$match': {
            '$or': [
                {'filetype': 'f'},
                {'filetype': 'l'}
            ]
        }
    }, {
        '$project': {
            'filename': 1, 
            'filepath': 1, 
            'filesize': 1, 
            'filetype': 1
        }
    }
    ]

    src_results = connection_to_src_filter.aggregate(aggregation)
    return src_results



main()