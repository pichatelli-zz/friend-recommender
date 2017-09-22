#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask
from flask import request,Response,json
from py2neo import watch
from dbhandler import neo4jhandler
import os
import sys
import json
import datetime
import logging
import logging.config

app = Flask(__name__)

#setting neo4j log levels
watch("neo4j.bolt", level='WARN')
watch("neo4j.http", level='WARN')

logger = logging.getLogger(__name__)

#Utilitary class used to transform database return into object
class Person(object):
  def __init__(self, id, name):
      self.id, self.name = id, name

def main():
    logger.info('Starting friend manager API')
    app.run()
    logger.info('Terminating friend manager API')

#import logging configs
def setup_logging():
    path = 'log_configs.json'
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)

#Returns a list of friends for each given person_id
@app.route("/api/v2/friends/list", methods=['GET'])
def get_friend_list():
    id_list = map(int,request.args.get('id').split(','))
    logger.info('Processing friend_list request with params %s', id_list)
    logger.debug('Getting database connection')
    graph = neo4jhandler.getconnection()
    query_result = list(neo4jhandler.getfriendlist(id_list, graph))
    logger.debug('Parsing query results')
    personlist = to_personlist(query_result)
    logger.debug('Building response based on list %s', personlist)
    response_data = build_fl_response(personlist)
    resp_status = 200 if len(personlist) > 0 else 204
    logger.info('Sending friend_list response with status %s', resp_status)
    response = Response(response=response_data, status=resp_status, mimetype="application/json")
    return response

#Builds a personlist using neo4j return
def to_personlist(query_result):
    personlist = []
    for record in query_result:
        person = Person(id=record.get('person').get('id'), name=record.get('person').get('name'))
        friend = Person(id=record.get('friend').get('id'), name=record.get('friend').get('name'))

        list_item = next((item for item in personlist if item['person'].id == record.get('person').get('id')), None)
        if list_item == None:
            personlist.append({'person':person,'friend_list':[friend]})
        else:
            list_item.get('friend_list').append(friend)

    return personlist

#Builds friend list response
def build_fl_response(personlist):
    friendjson = {
        'hits': len(personlist),
        'data': []
    }
    for person in personlist:
        friend_list = []
        item = person['person'].__dict__
        for friend in person['friend_list']:
            friend_list.append(friend.__dict__)

        item.update({'friend_list':friend_list})
        friendjson.get('data').append(item)

    return json.dumps(friendjson)

#returns a friend suggestion list along with all common each suggestion
@app.route("/api/v2/<id>/friendships/suggested", methods=['GET'])
def get_suggested_friends(id):
    logger.info('Processing friends suggestion request for id %s', id)
    logger.debug('Getting database connection')
    graph = neo4jhandler.getconnection()
    query_result = list(neo4jhandler.getsuggestedfriends(int(id), graph))
    logger.debug('Parsing query results')
    suggestedlist = to_suggestedlist(query_result)
    logger.debug('Building friends suggestion response on %s', suggestedlist)
    response_data = build_suggested_response(suggestedlist)
    resp_status = 200 if len(suggestedlist) > 0 else 204
    logger.info('Sending suggestion response with status %s', resp_status)
    response = Response(response=response_data, status=resp_status, mimetype="application/json")
    return response

#transforms database return into objects list
def to_suggestedlist(query_result):
    suggestedlist = []
    #Trata sugestoes
    for record in query_result:
        suggested_friend = Person(id=record.get('suggested').get('id'), name=record.get('suggested').get('name'))
        common_friend = Person(id=record.get('common_friend').get('id'), name=record.get('common_friend').get('name'))
        my_item = next((item for item in suggestedlist if item['suggested'].id == record.get('suggested').get('id')), None)
        if my_item == None:
            suggestedlist.append({'suggested':suggested_friend, 'common_friends':[common_friend]})
        else:
            my_item.get('common_friends').append(common_friend)

    return suggestedlist

#builds response for suggested friends method
def build_suggested_response(suggestedlist):
    friendjson = {
        'hits': len(suggestedlist),
        'data': []
    }
    for person in suggestedlist:
        friend_list = []
        item = person['suggested'].__dict__
        for friend in person['common_friends']:
            friend_list.append(friend.__dict__)

        item.update({'common_friends':friend_list})
        friendjson.get('data').append(item)

    return json.dumps(friendjson)

# Create a relationship between two nodes
# After creating the new relationship, reprocess both suggestions list
@app.route("/api/v2/friendships/create", methods=['POST'])
def create_new_friendship():
    logger.info('Processing new friendship request')
    request_body = request.get_json(request.data)
    logger.debug('Request body %s', request_body)
    logger.debug('Getting database connection')
    graph = neo4jhandler.getconnection()
    logger.debug('Performing database changes')
    neo4jhandler.create_new_friendship(request_body, graph)
    neo4jhandler.update_recommended_friends(request_body['id_person'], graph)
    neo4jhandler.update_recommended_friends(request_body['id_person'], graph)
    logger.info('Sending new friendship response')
    response = Response(status=201, mimetype="application/json")
    return response

# Adds a new person to the database
@app.route("/api/v2/person/create", methods=['POST'])
def create_new_person():
    logger.info('Processing new person request')
    request_body = request.get_json(request.data)
    logger.debug('Request body %s', request_body)
    logger.debug('Getting database connection')
    graph = neo4jhandler.getconnection()
    logger.debug('Performing database changes')
    neo4jhandler.create_person(request_body, graph)
    logger.info('Sending new person response')
    response = Response(status=201, mimetype="application/json")
    return response

if __name__ == "__main__":
    setup_logging()
    main()
