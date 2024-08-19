CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Thoughts on schema design:
--
-- * Job descriptions don't change, so store those independently?
--   * Those need to be bounded somehow too
-- * Have an executions table that describes each attempt
-- * Try to put different stages into different tables? Keeps a small working table for
--   things that are running
-- * Does it make sense to have a separate table per worker? Worker
--   assignments? Maybe depends on how many workers there are? For fewer
--   workers, no.
-- * Need acknowledgements if we are assigning jobs to workers
-- * How much happens in the database versus outside?
--
-- * Want to deal with bloat over time... I guess we get some high-water-mark
--   behavior that we'd need to consider with many tables.
-- * Can we make it easy to adjust as we go with partitioning?
-- * Can we make one queue not affect others?
-- * Jobs that aren't immediately ready to run should go in a separate "staging" table
--
-- enqueued --> assigned --> processing --> success or failure --> retry?
-- (table)           (worker table)         (archive)

-- Every job description goes into this table.
CREATE TABLE job (
  id              uuid NOT NULL PRIMARY KEY,
  owner_id        uuid NOT NULL,
  kind            varchar(255) NOT NULL,
  max_attempts    smallint NOT NULL DEFAULT 1,
  timeout         interval NOT NULL,
  create_time     timestamptz NOT NULL,
  schedule_time   timestamptz,
  blocking_job_id uuid REFERENCES job (id),
  queue           varchar(255),
  arguments       jsonb NOT NULL,

  -- Ensure that FIFO queue and blocking job mechanisms are not used together. This
  -- limitation makes for more predictability and I don't think this is a problem in
  -- practice.
  CHECK (queue IS NULL OR blocking_job_id IS NULL)
);

CREATE TABLE worker (
  id             uuid PRIMARY KEY,
  name           varchar(255) NOT NULL,
  register_time  timestamptz NOT NULL,
  last_ping_time timestamptz NOT NULL
);

CREATE TYPE job_state AS ENUM (
  'ENQUEUED',
  'PROCESSING'
);

CREATE TYPE job_outcome AS ENUM (
  'SUCCEEDED',
  'FAILED',
  'CANCELLED'
);

-- Any job that has a set `schedule_time` needs to be inserted into this table. A worker
-- will periodically pull jobs that are ready to start into the `running_job` table.
CREATE TABLE scheduled_job (
  job_id        uuid NOT NULL REFERENCES job (id),
  schedule_time timestamptz NOT NULL
);

-- It's OK to keep these indexed in a btree. We won't be making any updates to this
-- table so no need to optimize for HOT updates.
CREATE INDEX ON scheduled_job (schedule_time);

-- Any job that depends on another job completing before it can be run should go in this
-- table. When the blocking job completes in `running_job`, entries depending on that in
-- this table will be moved into `running_job`.
CREATE TABLE blocked_job (
  job_id uuid NOT NULL REFERENCES job (id) PRIMARY KEY,
  blocking_job_id uuid NOT NULL REFERENCES job (id),

  CHECK (job_id != blocking_job_id)
);

CREATE INDEX ON blocked_job (blocking_job_id);

CREATE TABLE queued_job (
  queue varchar(255) NOT NULL,
  position bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
  job_id uuid NOT NULL REFERENCES job (id),

  PRIMARY KEY (queue, position)
);

CREATE UNIQUE INDEX ON queued_job (job_id);

-- This keeps track of jobs that are currently executing. It needs to be designed in a
-- way that preserves HOT updates, so avoid mutable fields with non-summary indexes.
CREATE TABLE running_job (
  job_id      uuid NOT NULL REFERENCES job (id),
  attempt     smallint NOT NULL,
  state       job_state NOT NULL,
  worker_id   uuid,
  start_time  timestamptz,
  finish_time timestamptz,
  queue       varchar(255) UNIQUE,

  PRIMARY KEY (job_id, attempt),
  CHECK (attempt > 0),
  CHECK (worker_id IS NULL OR (state <> 'ENQUEUED' AND start_time IS NOT NULL))
);

-- Finished job attempts are placed in this table, providing a "receipt" for the job.
-- This can be periodically cleaned up by dropping partitions.
CREATE TABLE finished_job (
  job_id      uuid NOT NULL REFERENCES job (id),
  attempt     smallint NOT NULL,
  outcome     job_outcome NOT NULL,
  start_time  timestamptz,
  finish_time timestamptz,
  result      text,

  CHECK (attempt > 0)
) PARTITION BY RANGE (outcome, start_time);


-- Create a default partition for local development.
CREATE TABLE finished_job_default PARTITION OF finished_job DEFAULT;

-- Here's an example partition.
CREATE TABLE successful_job_20240718 PARTITION OF finished_job FOR VALUES FROM ('SUCCEEDED', '2024-07-18') TO ('SUCCEEDED', '2024-07-19');

-- It has an index on job_id to make lookups more efficient.
CREATE INDEX ON successful_job_20240718 (job_id);

CREATE TABLE successful_job_20240719 PARTITION OF finished_job FOR VALUES FROM ('SUCCEEDED', '2024-07-19') TO ('SUCCEEDED', '2024-07-20');
CREATE INDEX ON successful_job_20240719 (job_id);
