# -*- coding: utf-8 -*-
from py2neo import authenticate, Graph
import os
import sys


def getdbparams():
    __location__ = sys.path[0]
    params = {}
    with open(os.path.join(__location__, 'db_params.txt'), 'r') as db_params:
        for line in db_params.readlines():
            key, part, value = line.partition('=')
            params.update({key:value.rstrip()})
    return params

def getconnection():
    dbparams = getdbparams()
    authenticate(dbparams['neo4j_host_port'], dbparams['neo4j_username'], dbparams['neo4j_password'])
    graph = Graph(dbparams['neo4j_connection_url'])
    return graph

def getsuggestedfriends(person_id, graph):
    query = '''
        MATCH (person:Person {id:{id}})-[:SUGGESTED]-(suggested:Person)
        MATCH (person:Person)-[:KNOWS]-(common_friend:Person)-[:KNOWS]-(suggested:Person)
        RETURN suggested, common_friend
    '''
    result = graph.run(query, parameters={'id':person_id})
    return result

def getfriendlist(id_list, graph):
    query = '''
        MATCH (person:Person)-[:KNOWS]-(friend:Person)
        WHERE person.id IN {id_list}
        RETURN person, friend;
    '''
    result = graph.run(query, parameters={"id_list":id_list})
    return result

def create_new_friendship(params, graph):
    query = '''
        MATCH (person:Person{id:$id_person}),(new_friend:Person{id:$id_friend})
        OPTIONAL MATCH (person)-[rel]-(new_friend)
        WHERE type(rel) = 'SUGGESTED'
        MERGE (person)-[:KNOWS]->(new_friend)
        DELETE rel
    '''
    graph.run(query, parameters=params)

def update_recommended_friends(person_id, graph):
    query = '''
        MATCH (person:Person{id:{person_id}})-[:KNOWS]-(friend:Person)-[:KNOWS]-(suggested_friend:Person)
        WHERE NOT (person)-[:KNOWS]-(suggested_friend) AND person <> suggested_friend
        MERGE (person)-[suggested:SUGGESTED]-(suggested_friend)
        RETURN person.name, type(suggested), suggested_friend.name
    '''
    graph.run(query, parameters={"person_id":person_id})

def create_person(params, graph):
    query = '''
        CREATE (person:Person{id:$id, name:$name})
        WITH person
        UNWIND $friends_id AS friend_id
        MATCH (friend:Person{id:friend_id})
        MERGE (person)-[:KNOWS]->(friend)
    '''
    graph.run(query, parameters=params)
