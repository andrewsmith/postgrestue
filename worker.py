#!/usr/bin/env python3
"""An example implementation of a worker that uses the mailbox schema."""

from dataclasses import dataclass
import json
import sys

import psycopg


@dataclass
class Job:
    pass


async def process_one_job(conn):
    # Select for update from running_job
    # Mark the job as being processed by this worker
    try:
        # Invoke it
    except:
        # Create a new attempt if not exceeding max_attempts
        # Insert this one into finished_job
    else:
        # Delete it from running_job and insert it into finished_job
        # Do any bookkeeping necessary
        #   If the job is blocking anything else, move those to running_job
        #   If the job came from a queue, see if there is a next job to pull into running_job


async def move_scheduled_jobs_to_running(conn):
    while True:
        # Select all jobs that have a scheduled start time earlier than now
        # Move them in batches to running_job


def main(args):
    # Set up an async pool
    # Set up a connection
    # Start move_scheduled_jobs_to_running
    # Loop through processing jobs


if __name__ == "__main__":
    main(sys.argv[1:])
