import random

from locust import FastHttpUser, task


SKUS = [
    "bicycle",
    "lawnmower",
    "shovel",
    "car",
    "battery",
    "rake",
    "sled",
]


class OrderUser(FastHttpUser):
    @task
    def order(self):
        self.client.post(
            "/place_order",
            json={"sku": random.choice(SKUS), "quantity": random.randrange(1, 5)}
        )
