#!/usr/bin/env python3

import time
import redis
from celery import Celery, chord

app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

is_fresh_dict = {}

nodes = {
  'A': [],
  'B': [],
  'C': ['A'],
  'D': ['A', 'B'],
  'E': ['C'],
  'F': ['D', 'E'],
  'G': ['F']
}

@app.task
def refresh(dataset):
  print('refreshing ' + dataset)
  time.sleep(5)
  is_fresh_dict[dataset] = True

def is_fresh(dataset):
  return is_fresh_dict.get(dataset, False)

def can_refresh(dataset):
  node_parents = nodes[dataset]
  for key in is_fresh_dict:
    if is_fresh_dict[key]:
      if dataset == key:
        return False
      elif key in node_parents:
        node_parents.remove(key)
  return not node_parents

# @app.task
# def refresh_with_ancestors(dataset):
#   tasks = []
#   parents = nodes[dataset]

#   # if any parent of dataset are not fresh,
#   # we add them to our tasks list
#   for node in parents:
#     if is_fresh(node) == False:
#       tasks.append(refresh.s((node)))

#   tasks = [tasks[0]]

#   # if all parents of dataset are fresh,
#   # we simply refresh the dataset
#   if len(tasks) == 0:
#     result = refresh.delay(dataset)
#     while not result.ready():
#       time.sleep(1)
#     return

#   # if all parents of dataset are not fresh,
#   # refresh all the nodes from the top of the graph
#   elif len(tasks) == len(parents):
#     pass

#   # if *some* parents of the dataset are not fresh,
#   # refresh the not fresh, and then refresh the dataset
#   else:
#     callback = refresh.s()
#     result = chord(tasks)(callback)
#     while not result.ready():
#       time.sleep(1)
#     return

#   return

@app.task
def refresh_nodes():
  for key in nodes:
    if can_refresh(key):
      refresh.apply_async(args=[key], link=refresh_nodes.si())

if __name__ == '__main__':
  refresh_nodes()
  #print(can_refresh('C', {'A': True}, nodes))
