from locust import FastHttpUser, task


class RegisterUser(FastHttpUser):
    @task
    def register(self):
        self.client.post("/register", json={"name": "Andrew"})
