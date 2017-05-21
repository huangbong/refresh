#!/usr/bin/env python3

import time
from celery import Celery

app = Celery('refresh', broker='redis://localhost')

to_refresh = {}

nodes = {
  'A': [],
  'B': [],
  'C': ['A'],
  'D': ['A', 'B'],
  'E': ['C'],
  'F': ['D', 'E'],
'G': ['F'] }

@app.task
def refresh(dataset):
  time.sleep(5)
  global to_refresh
  to_refresh[dataset] = False

def should_refresh(dataset):
  return to_refresh.get(dataset, True)

def refresh_with_ancestors(dataset):
  print(should_refresh('F'))
  pass

if __name__ == '__main__':
  print(should_refresh('E'))
  refresh.delay('F')