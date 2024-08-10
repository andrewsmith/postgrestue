from locust import HttpUser, task


class RegisterUser(HttpUser):
    @task
    def register(self):
        self.client.post("/register", json={"name": "Andrew"})
