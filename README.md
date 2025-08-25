# keepoid

Keepoid is a 3rd party extension to [sanoid](https://github.com/jimsalterjrs/sanoid), focusing on intelligent snapshot management on the `syncoid` destination site while permitting minimized snapshots on the source. Keepoid uses a fundimentally different way to think about snapshot management and pruning, and borrows retention philosophies from [pgBackRest](https://github.com/pgbackrest/pgbackrest).

Keepoid itself does not take snapshots. Instead, it focuses on retention and pruining of snapshots created by sanoid and/or replicated with syncoid. keepoid can be thought of as the system that determines which snapshots should be kept, and which should be auto-pruned.

## Use-case

We'll use the example of using syncoid to replicte snapshots from a source zfs dataset to a remote archive server. It is desired for the source dataset to have as few snapshots as possible, so `sanoid` is not used. `syncoid` replicates the source dataset to the archive server, which creates a chain of incremental snapshots. It is intended for the archive server to store all daily/weekly/monthly snapshots, but these cannot be created on the backup site as syncoid will destroy separately-created snapshots when replication ocurrs.

Intead, the source server takes frequent and aggressively-pruned snapshots using `sanoid`, which are then replicated using `syncoid`. On the backup host, `keepoid` will handle tracking of which snapshots are requred to meet the desired retention periods, and prune all other snapshots.

This is made possible by having `sanoid` take snapshots at the minimum retention interval for `keepoid` (for example, retaining 36 hourly snapshot requires that `sanoid` on the source takes snapshots every hour). `sanoid` on the source should have a retention period that ensures that `syncoid` will replicate the snapshot to the destination for `syncoid` to manage.

## Configuring keepoid retention

All times use a suffix to define the unit. Valid suffixes are:

- Seconds: s
- Minutes: m
- Hours: h
- Days: d

```yaml
# /etc/keepoid/keepoid.conf

# The time to anchor snapshot retention settings to (24-hour time)
# Setting startTime is useful when it's desired to offset when a snapshot should be retained
# For example, it may be desired to retain daily snapshots at 16:00 instead of 00:00
# startTime can also be set for a specific retention setting
startTime: 00:00

# How long a snapshot should be kept around before it is elegible for pruning.
# To avoid syncoid re-syncing snapshots multiple times, this should be at least
# the retention window defined by sanoid.
# This ensures sanoid prunes the snapshot before keepoid does.
pruneAfter: 1h

# The path to look for snapshots at
path: example/dataset/path

# If true, skips the parent folder (e.g. "path", from above)
skipParent: false

# The snapshot prefix to consider for retention settings
identifier: autosnap_

# A list of retention policies. Each setting can define:
#   interval: The interval snapshots should be retained at
#   count: The number of snapshots to retain for this interval
#   startTime: (Optional) The 24-time to anchor the interval to
#   path: (Optional) A custom path for this retention policy
retention:
  - interval: 1h  # Retain a snapshot for every hour
    count: 24     # Retain 24 hourly snapshots
  - interval: 6h  # Retain a snapshot for every 6th hour
    count: 42     # Retain 42 6-hourly snapshots (7 days)
  - interval: 1d  # Retain a snapshot for every day
    count: 30     # Retain 30 daily snapshots
  - interval: 30d # Retain a snapshot for every 30 days
    count: 3      # Retain 3 30-day snapshots (90 days)
```

## Retention and pruning

Snapshot intervals define guranteed retention setting. For example, consider the following retention policy, which states that 1 snapshot should be retained for every 30-day interval.

```yaml
startTime: 00:00
retention:
  - interval: 24h # Retain a snapshot for every 24 hours
    count: 1      # Retain 1 daily snapshots
```

When does this snapshot get pruned? Consider the following snapshot schedule:

1. Monday: 00:00
2. Tuesday: 00:00
3. Wednesday: 00:00

It would be conterintuitive to prune snapshot 1 when snapshot 2 is created, because we lose the ability to rollback to ~24 hours out. Instead, it is guranteed that the defined 24-hour retention window (`count * interval`) will always fall *within* the available snapshots. This means that snapshot 1 will only be pruned when another snapshot meets or exceeds the configured retention window.

## Installation

`keepoid` requires Python 3.8+

1. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2. Copy the `keepoid.py` script to a location in your system's PATH, for example:

    ```bash
    sudo cp keepoid.py /usr/local/bin/keepoid
    ```

3. Make the script executable:

    ```bash
    sudo chmod +x /usr/local/bin/keepoid
    ```

It's recommended to create an entry in crontab for keepoid to run regularly

## Development

For local development and testing, it is recommended to use a Python virtual environment.

1. Clone the repository:

    ```bash
    git clone https://github.com/drewburr-labs/keepoid.git
    cd keepoid
    ```

2. Create and activate a virtual environment:

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. Install the required dependencies for development and testing:

    ```bash
    pip install pyyaml pytest pytest-mock
    ```

4. Run the test suite:

    ```bash
    pytest
    ```
