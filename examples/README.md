# Keepoid Example Use-Case

This example demonstrates how to use `keepoid` to manage ZFS snapshot retention for a dataset using a simple configuration.

## Example Scenario

Suppose you have a dataset called `pool/data` and you are taking hourly snapshots using a tool like `sanoid` or a cron job. Over time, you accumulate many snapshots and want to keep only the most relevant ones according to a retention policy.

## Example Files

- [`keepoid.conf`](./keepoid.conf): Example configuration file for `keepoid`.
- [`zfs_snapshots.txt`](./zfs_snapshots.txt): Simulated output of `zfs list -t snapshot -p -r pool/data` showing 72 hourly snapshots.

## Configuration

The configuration below tells `keepoid` to keep only the last 2 hourly and last 2 daily snapshots for `pool/data`:

```yaml
# keepoid.conf
startTime: "00:00"
pruneAfter: "1m"
path: "pool/data"
identifier: "autosnap_"
retention:
  - interval: 1h
    count: 2
  - interval: 1d
    count: 2
```

- `startTime`: The anchor time for daily retention (midnight).
- `pruneAfter`: Snapshots not matching a policy are eligible for pruning after 1 minute.
- `path`: The dataset to manage.
- `identifier`: Only snapshots with this prefix are considered.
- `retention`: Keep 2 hourly and 2 daily snapshots.

## How It Works

1. **Discovery**: `keepoid` lists all snapshots for `pool/data` matching the `identifier`.
2. **Grouping**: Snapshots are grouped by dataset (in this example, just `pool/data`).
3. **Policy Application**: For each group, `keepoid`:
   - Finds the most recent 2 hourly snapshots (e.g., at 14:00 and 13:00).
   - Finds the most recent 2 daily snapshots at or after midnight (e.g., at 00:00 for today and yesterday).
   - Keeps the closest snapshot to each interval anchor; if a snapshot matches both hourly and daily, it is only kept once.
4. **Pruning**: All other snapshots older than `pruneAfter` are pruned.

## Example Outcome

Given 72 hourly snapshots from `2023-10-24T15:00:00` to `2023-10-27T14:00:00`, running `keepoid` with this config at `2023-10-27T14:00:00` will result in:

- **Kept**:
  - `pool/data@autosnap_2023-10-27T14:00:00`
  - `pool/data@autosnap_2023-10-27T13:00:00`
  - `pool/data@autosnap_2023-10-27T00:00:00`
  - `pool/data@autosnap_2023-10-26T00:00:00`
- **Pruned**: The remaining 68 snapshots.

## Running the Example

1. Place your config as `keepoid.conf` and ensure your ZFS snapshots match the naming pattern.
2. Run:

   ```bash
   keepoid --config ./keepoid.conf --dry-run
   ```

   The `--dry-run` flag will show which snapshots would be pruned without actually deleting them.

This demonstrates how `keepoid` intelligently selects snapshots to keep based on your retention policy, minimizing storage use while maximizing restore flexibility.
