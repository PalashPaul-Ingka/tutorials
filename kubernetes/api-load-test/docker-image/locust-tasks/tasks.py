#!/usr/bin/env python
from datetime import datetime
from locust import HttpLocust, TaskSet, task
import products
import random

class ReviewTaskSet(TaskSet):
    _data = None
    
    def get_param(self):
        return random.choice(self._data)

    def on_start(self):
        self._data = products.get_products()

    @task(999)
    def post_metrics(self):
        param = self.get_param()
        self.client.get(f"/<path>/{param['param1']}/{param['param2']}/{param['param3']}" ,name =f"/<path>/{param['param1']}")

class UserLocust(HttpLocust):
    task_set = ReviewTaskSet