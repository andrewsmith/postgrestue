import random

from locust import FastHttpUser, task


class SleepUser(FastHttpUser):
    @task
    def sleep(self):
        self.client.post("/sleep", json={"seconds": random.uniform(0.1, 0.5)})
