#!/usr/bin/env python

import argparse
import json
import sys
import csv
import xml.etree.ElementTree as ET
from watson_developer_cloud import RetrieveAndRankV1


def get_retrieve_and_rank(username, password):
    retrieve_and_rank = RetrieveAndRankV1(
        username=username,
        password=password)
    return retrieve_and_rank


def index_documents(retrieve_and_rank, solr_cluster_id, collection_name, docs):
    status = {}
    try:
        pysolr_client = retrieve_and_rank.get_pysolr_client(solr_cluster_id, collection_name)
        client_resp = pysolr_client.add(docs)
        root = ET.fromstring(client_resp)
        stat = root.find('./lst/int[@name="status"]')
        if stat is not None and stat.text == "0":
            status["status"] = "success"
        else:
            status["status"] = "error"
    except:
        print sys.exc_info()
        status["status"] = "error"
    return status


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True, dest="filename", help="bug file to index")
    parser.add_argument("-u", "--username", required=True, dest="username", help="Retrive and Rank username")
    parser.add_argument("-p", "--password", required=True, dest="password", help="Retrive and Rank password")
    parser.add_argument("-s", "--solr_cluster_id", required=True, dest="cluster",
                        help="Retrive and Rank solr cluster id")
    parser.add_argument("-c", "--solr_collection", required=True, dest="collection",
                        help="Retrive and Rank solr collection name")
    args = parser.parse_args()
    docs = []
    retrieve_and_rank = get_retrieve_and_rank(args.username, args.password)
    with open(args.filename, "r") as f:
        freader = csv.reader(f, delimiter='\t')
        for row in freader:
            doc = dict()
            doc["id"] = row[0]
            doc["title"] = row[1]
            doc["bugid"] = row[1]
            doc["body"] = row[2]
            docs.append(doc)
    status = index_documents(retrieve_and_rank, args.cluster, args.collection, docs)
    print "index status - " + json.dumps(status)


if __name__ == '__main__':
    main()
