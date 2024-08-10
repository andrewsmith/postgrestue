Load Testing
============

This is a log of my testing and tuning to see how much performance I can get
out of this implementation of a PostgreSQL-based job queue.

Round #1
--------
The setup for this has been:
1. Design the Postgres schema in a way that seems efficient. There is more
   context in `schema.sql`, but generally this is separate job description
   from job execution, look for opportunities to get HOT updates, design
   bookkeeping structures to be updated as a job is processed.
2. Implement the code portion of this in Python using psycopg in async mode.
3. Write a Starlette-based HTTP server to create a web API. Use hypercorn and
   do connection pooling (max 10 connections) to PostgreSQL. This is closer to
   real-world usage and would allow me to switch load generation tools, if
   desired.
4. Set up Locust to enqueue jobs via the web API.

I am running these on my 2020 Apple M1 MacBook Air with 8GiB RAM.

I've done a few runs gradually increasing the number of users and reached
best results at:
* 8 concurrent "users"
* Throughput of ~2530 RPS
* p95 response times of 4ms

I'd consider these "best" because this was the maximal throughput for which
response times stayed low and stable. Beyond this (16 users), throughput dropped
and response times climbed.

This seems like a decent starting point, but I imagine there's a fair bit more
performance on the table. In particular, I don't seem to be saturating CPU usage
when doing these runs.

Round #2
--------
My first change was to increase the connection pool size from max=10 to max=20.
My hypothesis is that requests might be waiting on connections to become
available before they could be processed. However, upon running with this, I see
no improvement at 16 users.

Round #3
--------
I switched to using 4 workers with hypercorn. This got to an improved new best
result at:
* 16 concurrent users
* Throughput peaking at ~3200 RPS and averaging above 3000 RPS
* p95 response time of 6ms

I was hitting higher >90% CPU for the load generator process.
