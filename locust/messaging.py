import random

from locust import FastHttpUser, task


MESSAGES = [
    "Hello",
    "Here's an example message",
    "And another message",
    "What if there is something else",
    "How can you do it if this is all",
    "All of this is a facade",
    "How are you?",
]


class MessagingUser(FastHttpUser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parent_id = None

    @task
    def order(self):
        request_body = {"message": random.choice(MESSAGES)}
        if self.parent_id:
            request_body["parent_id"] = self.parent_id
        response = self.client.post(
            "/message",
            json=request_body
        )
        response_body = response.json()
        self.parent_id = response_body["job_id"]
