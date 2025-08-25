# Pull-based backups

The files in the directory describe how pull-based backups can be achieved using [sanoid](https://github.com/jimsalterjrs/sanoid/wiki/Sanoid), [syncoid](https://github.com/jimsalterjrs/sanoid/wiki/Syncoid), and keepoid. The existing files provide:

- A source zfs pool with only one hourly snapshot for each dataset ([sanoid.conf](./sanoid.conf))
- A destination server hosting a mirror of the source pool, using pull-based replication every 30 minutes ([syncoid-cron](./syncoid-cron))
- A destination pool with easily-defined snapshot retention policies, managed independently from the source pool ([keepoid.yaml](./keepoid.yaml))

Since the source pool only contains one snapshot, the volume of storage required on the source is minimized. Aside from this one snapshot, the destination server is responsible for the entire volume of data required for snapshots.

## Customizing

To start, determine the retention policies desired. [keepoid.yaml](./keepoid.yaml) can be used as a template.

Once the policies are decided, note the shortest retention interval (in our example, this is `1h`). This will be the rate at which [sanoid.conf](./sanoid.conf) should be configured to take backups.

> In the event an interval such as `30m` is desired, sanoid's `frequently` and `frequent_period` [options](https://github.com/jimsalterjrs/sanoid/wiki/Sanoid#options) can be used. Note this may be limiting if the chosen interval is not a factor of `60m`.

Finally, decide how syncoid should be run. The example [syncoid-cron](./syncoid-cron) is a simple cronjob that runs every 30 minutes. This means replication is happening every 30 minutes. Note that since snapshots are taken every hour, the additional halfway-between run of syncoid is purely for replication purposes and is not required.

Similarly, it may be desired to run syncoid less often. For example, snapshots taken every hour do not need to be replicated immediately. Syncoid could be run every 12 hours instead, repliicating all 12 hourly snapshots together. Note that this would require the number of snapshots retained to be increased in [sanoid.conf](./sanoid.conf).

## Installing

1. Install the sanoid package on both the source and destination servers.
    - sanoid contains both sanoid and syncoid
2. Install keepoid on the destination server
3. Setup [sanoid.conf](./sanoid.conf) on the source server to start taking snapshots
4. Setup [syncoid-cron](./syncoid-cron) on the destination server to start replicating the source pool.
5. Setup [keepoid-cron](./keepoid-cron) on the destination server to enable keepoid to manage your snapshots
    - Increase or descrease the cron interval as desired
