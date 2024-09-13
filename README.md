Postgrestue
===========

Postgrestue is a prototype of an async job processing system built on top of
PostgreSQL. Jobs can be enqueued by any clients connecting to the database and
worker tasks dequeue and process those jobs. The purpose of this project is
to explore ways to make this more performant than a similar system we use at
work.

Load testing of this system on my 2020 M1 MacBook Air with 8GiB of RAM yields
the ability to process >1000 jobs per second for simple jobs that logged a
single log line. Enqueue and dequeue times are consistently around 2-3ms at the
95th percentile. This stayed consistent even with a greater backlog (several
hundred thousand) of jobs. Given a dedicated database server and multiple
worker machines I believe this can scale to even higher performance figures.

Features
--------

This prototype supports the following features:

* Enqueue and asynchronously process jobs that include the function name to
  execute (`kind`), a key-value set of arguments (`arguments`), an owner for
  the purpose of attribution (`owner_id`), a maximum number of attempts before
  giving up (`max_attempts`), and a time limit for how long the job can run
  (`timeout`).
* Schedule jobs to run at a specific time in the future (`schedule_time`).
* Mark jobs as blocked by another job and have those jobs run upon that job's
  successful completion (`blocked_job_id`).
* Process jobs that belong to a named queue in FIFO order, running one job
  per queue at a time.
* Know which worker a job is assigned to. In the future, this could lead to
  rescheduling jobs away from a worker that is no longer responding.

Design
------

I will cover the design choices that I believe are most important towards the
performance of this system.

#### 1. Separate job description from job execution

This prototype puts unchanging, descriptive information about a job in its own
table (`job`). This is then referenced often elsewhere via foreign key
relationships. The execution of that job is described in `running_job`. Those
details are subject to change: the current status, attempt number, and worker.

This is different from the design of its predecessor, which holds both
description and execution information in one table. Whenever one value is
changed for a job, the whole row/tuple is copied into a new heap entry with
that one value changed. Longer string and binary values that are stored in
toast tables also need to be copied. This makes a common operation like
updating the status of the job from pending to executing expensive.

The `job` table can be mostly treated as append-only/immutable. Its contents
might be larger if there are a number of arguments supplied to the job, but
this doesn't need to be copied ever. `running_job` holds a small number of
(mostly) fixed-size datums, which led to less expensive updates.

This also means that `job` can be indexed and doesn't require reindexing
whenever an execution value changes in `running_job`. This further cuts down on
the number of writes that need to happen when updating a job's execution.

Separating these also allows us to represent multiple attempts at executing the
job without losing information about previous attempts. Each attempt gets its
own row with an incremented `attempt` number for that job ID. `completed_job`
uses job ID and attempt to uniquely identify a specific execution.

#### 2. Keep the "working set" separate

Fundementally, we can consider jobs to fall into one of three sets:

1. Those that cannot be run until some condition is met
2. Those that are runnable now or currently running
3. Those that have completed

Rather than combining all of these in one table, this prototype splits them
up into those groups. That allows #2 (`running_job`) to stay comprised of jobs
that are running or can be run without meeting any additional conditions. Jobs
that are not ready to run yet go into different "staging" tables, depending on
what is preventing them from being able to run. Once a job execution is
finished, it will be moved to the `finished_job` table. That table serves as
medium-to-long term storage for completed jobs. It is partitioned by outcome
and timestamp, allowing the potential for daily partitions that can be detached
and archived/deleted after an acceptable amount of time.

Having `running_job` as its own table allows the dequeue query to be simple and
highly performant. It becomes a simple heap sequential scan that only needs to
jump over the currently processing or locked rows, which is proportional to the
number of workers.  With appropriate autovacuuming, the table's heap will also
stay small.

It also reduces the need for indexes on that table because it is smaller and
only contains data that meets its conditions (running or runnable jobs).
This leads to cheaper writes on that table.

#### 3. Choose near-constant cost operations whenever possible

The necessary bookkeeping for a job either happens when enqueuing that job or
when a blocking job completes. This extra write cost allows reads, or more
specifically, the dequeue query to be cheap, since it doesn't need to do any
expensive operations to figure out the next job to process. The write
operations tend to be `O(1)` or `O(log(N))` in time complexity. Dequeue can
then be `O(K)`, where `K` is the number of workers currently processing jobs.
In the worst case, a sequential heap scan will skip over the currently
processing and locked jobs to find an available one to process. While this is
a scan, it should be a fairly inexpensive and avoid needing to use indexes.

In order to have near-constant cost operations, the bookkeeping steps need to
be carefully considered. During enqueue, we know enough about a job to put it
into the supporting tables (`scheduled_job`, `queued_job`, or `blocked_job`).
There is a `postgrestue.scheduler` module that finds jobs that are ready to run
and moves those into `running_job` with a `O(J * log(N))` cost where `J` is
the number of jobs that are ready to run and `N` is the total number of
scheduled jobs. Unblocking jobs happens when a job finishes and is also
`O(J * log(N))`. FIFO jobs are particularly interesting: they are
opportunistically placed directly into `running_job` if no other job for that
queue is running, otherwise, they just have a record in `queued_job`. When
finishing a job for a particular queue, `queued_job` is searched for the next
job in that queue.

This boils down to a straightforward observation: dequeue is an oft-invoked
operation. Workers attempt it many times. Avoiding conditional logic in it
helps keep it fast and predictable even under load.

#### 4. Avoid overlapping conditions for jobs

This design choice is mainly one of simplicity. One is able to use scheduling,
blocking, or FIFO queuing of their job, but may not combine these for the same
job. It might be possible to support these in combination (e.g. a job that
waits for both a scheduled time and a blocking job to complete), but those
implementations seem complex and easy to introduce errors in. It also seems
harder to reason about. I didn't check, but I don't believe we use those in
overlapping ways today.

#### 5. Avoid index writes with updates

This goes hand-in-hand with #1 and #2. When a row is updated, any non-
summarizing indexes (basically all indexes including the typical `btree` ones)
need to be updated to point at the new row/tuple in the heap. This isn't just
for updates to the indexed column, but rather any column. This is because
non-summary indexes point the position within a heap block for the tuple. This
means updates lead to write amplification: not only does the new tuple need to
be written to the block, but also all indexes need to be updated to point to it.

One answer is HOT updates and only using summarizing indexes. [Heap-Only Tuples](
https://www.postgresql.org/docs/current/storage-hot.html) is about leaving
extra blank space in each block for updates so that updates can live in the
same block as the original insert. A summarizing index only points at the heap
block, so if that address doesn't change for the updated row, the index doesn't
need to be updated. The only index type that is summarizing in PostgreSQL
currently is BRIN (Block Range INdex).

Alternatively, one can avoid indexes, avoiding extra writes on both insert and
update as well as deletion. I have avoided indexing `state` in `running_job`.
Since this table stays relatively small, a sequential scan of the heap isn't
that expensive. There is an unique constraint and associated index on `queue` to
make sure that only one job per FIFO queue is running at a time. However, that
will be only used for jobs that use FIFO queues: null values shouldn't require
consulting or updating that index. `btree` is the only index that can be used
with unique constraints, so BRIN is out of the picture.

#### 6. Use SELECT FOR UPDATE and SKIP LOCKED when dequeuing

When multiple workers are dequeuing jobs quickly, they start to fight over the
same jobs. PostgreSQL has the ability to use row-level locking through
`SELECT ... FOR UPDATE` and `SKIP LOCKED` to serialize access to jobs. This
is the same as the current implementation, but I did experiment here. Row-level
locking involves writing to disk in PostgreSQL and that sounds slow compared to
the in-memory locking that comes with `SERIALIZABLE` isolation. I tried using
`SERIALIZABLE` instead of `SELECT FOR UPDATE`, but found the results to be
underperforming. There were frequent serialization failures that necessitated
application-side retries and the net result of those was that it was slower
than row-level locking (around 20% with just two workers). The balance of this
might change if dequeue was implemented as a PL/pgSQL function, but I haven't
explored that yet.

#### 7. Keep track of the workers

Workers are a first-class citizen in this data model: a row is inserted into
`worker` upon startup and removed upon shutdown. Workers ping periodically when
dequeuing jobs, so it is possibly to identify workers that are stuck. There's
a unique worker ID per async task in Python, so it is possible to observe
"stuckness" within a process. I think this aids in debugging and doesn't
harm performance.

#### 8. Dequeue one job at a time

I experimented with an approach that dequeues a set of jobs per worker process
and hands those off to async tasks. It was complicated, particularly in the
case of worker crashes/failure. This might be an avenue for better performance,
but the simplicity of each worker task dequeuing and processing one job at a
time is nice.

Open questions and exploration
------------------------------

While I feel pretty good about what I have discovered so far with this, there
are certainly more areas that need attention before one could use this in
production.

#### Performance testing

While I did a fair bit of load testing, it was mostly in short (<5 minute) runs
on my local machine. This should be tried:
* in a production-like environment
* with longer soak tests
* with real-world complications like long transaction times
* with deep (1M+) backlogs

#### The jury is out on pipelining

I have a branch of this that uses pipelining to send multiple statements to the
database server at once instead of blocking until the earlier ones complete.
This allows enqueue and post-execution bookkeeping to execute a few statements
at once. However, I saw a negative performance impact. I'm not currently sure
what the root cause is of that: it may be that there isn't any benefit when
running against local PostgreSQL and a slight amount of overhead. It might be
that the pipelining implementation in psycopg is still immature and needs
optimizing. This seems like one to benchmark in a more production-like
environment before enabling.

#### Give each queuing need its own set of tables and workers

A keen observer will note that this system has no "priority" field like the
current one we have at work. This was intentional and inspired by parts of the
"happy queues" talk. Priority gives an impression that workloads can be mixed
together successfully and achieve target latencies, but so long as there is a
homogeneous set of workers, a slow or crashing low-priority task can disrupt
the system.

The answer is that different queuing needs should have their own tables
(queues) and workers (compute resources). This implementation doesn't
explicitly support that, but I think adding a prefix to these tables, making
it easy to create a new set of them, and then telling workers which prefix to
use would enable this.

#### Ad-hoc query performance

I didn't focus much on the performance of ad-hoc queries like "How many
send_welcome_email jobs have been run?". I imagine these will degrade in speed
when many jobs exist. There could be an index on `job.kind` to support that. I
think that it would be worth considering what types of operations and queries
to support on this rather than indexing every conceivable column.

Conclusion
----------

This has been an interesting challenge and I have enjoyed trying to use
PostgreSQL in a way that does this efficiently. It has yielded some fun
puzzles. I've improved my knowledge about how PostgreSQL works. However, I do
think that this is inferior compared to adopting Temporal. Temporal has had a
lot more attention paid to performance. It separates the data plane and control
plane in a way that this and our current Postgres-based system doesn't. Having
the control plane in its own database is a win for stability and predictable
performance. Also, I created a lot of bugs while implementing this. Building
a good queuing system is hard. I don't claim that this is perfect and turning
it into a production system would be an additional feat. Because this doesn't
provide anything beyond what we get with Temporal, I really think we should
retire our existing system in favor of Temporal, not rewrite it with an approach
like this.
