#!/usr/bin/env python3

import argparse
import subprocess
import yaml
from datetime import datetime, timedelta, time

def parse_duration(duration_str):
    """Parses a duration string like '1h', '30d' into a timedelta."""
    unit = duration_str[-1]
    value = int(duration_str[:-1])
    if unit == "s":
        return timedelta(seconds=value)
    elif unit == "m":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    else:
        raise ValueError(f"Unknown duration unit: {unit}")


class Snapshot:
    def __init__(self, name: str, creation_time: datetime):
        self.name = name
        self.creation_time = creation_time

    def __repr__(self):
        return f"Snapshot(name='{self.name}', creation_time='{self.creation_time}')"


def get_snapshots(path, identifier, skip_parent=False):
    """Lists ZFS snapshots for a given path and filters by identifier."""
    cmd = [
        "zfs",
        "list",
        "-t",
        "snapshot",
        "-o",
        "name,creation",
        "-s",
        "creation",
        "-r",
        "-p",
        path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        lines = result.stdout.strip().split("\n")[1:]  # Skip header
        snapshots = []
        for line in lines:
            name, creation_epoch = line.strip().split()
            if f"@{identifier}" in name:
                if skip_parent and name.split("@")[0] == path:
                    continue
                creation_time = datetime.fromtimestamp(int(creation_epoch))
                snapshots.append(Snapshot(name, creation_time))
        return snapshots
    except FileNotFoundError:
        print("Error: 'zfs' command not found. Is ZFS installed and in your PATH?")
        return []
    except subprocess.CalledProcessError as e:
        print(f"Error listing snapshots for {path}: {e.stderr}")
        return []


def destroy_snapshot(snapshot_name, dry_run=False):
    """Destroys a ZFS snapshot."""
    if dry_run:
        print(f"DRY RUN: Would destroy snapshot {snapshot_name}")
        return

    print(f"Destroying snapshot {snapshot_name}")
    cmd = ["zfs", "destroy", snapshot_name]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error destroying snapshot {snapshot_name}: {e.stderr}")


def determine_snapshots_to_prune(
    all_snapshots: list[Snapshot],
    retention_policies,
    prune_after,
    global_start_time_str,
    now,
    dataset_path,
):
    """Determines which snapshots to keep and which to prune based on policies."""
    policy_kept_snapshots = set()

    # Filter policies for the current dataset
    applicable_policies = []
    for policy in retention_policies:
        policy_path = policy.get("path")
        if policy_path is None or policy_path == dataset_path:
            applicable_policies.append(policy)

    for policy in applicable_policies:
        interval = parse_duration(policy["interval"])
        # Add 1 to ensure backup window falls within available backups
        count = policy["count"] + 1
        start_time_str = policy.get("startTime", global_start_time_str)
        policy_start_time = time.fromisoformat(start_time_str)

        for i in range(count):
            # Anchor the target time calculation to create a datetime object
            anchor_date = now.date()
            if now.time() < policy_start_time:
                anchor_date -= timedelta(days=1)

            target_time = datetime.combine(anchor_date, policy_start_time) - (
                i * interval
            )
            best_snapshot = None
            min_diff = timedelta.max

            for snapshot in all_snapshots:
                if snapshot.creation_time >= target_time:
                    diff = snapshot.creation_time - target_time
                    if diff < min_diff:
                        min_diff = diff
                        best_snapshot = snapshot

            if best_snapshot:
                policy_kept_snapshots.add(best_snapshot)

    snapshots_to_prune = []
    prune_after_kept_snapshots = []
    for snapshot in all_snapshots:
        if snapshot not in policy_kept_snapshots:
            if now - snapshot.creation_time > prune_after:
                snapshots_to_prune.append(snapshot)
            else:
                prune_after_kept_snapshots.append(snapshot)

    return snapshots_to_prune, policy_kept_snapshots, prune_after_kept_snapshots


def group_snapshots_by_dataset(snapshots):
    """Groups snapshots by their dataset path (portion before '@')."""
    from collections import defaultdict

    grouped = defaultdict(list)
    for snapshot in snapshots:
        dataset = snapshot.name.split("@")[0]
        grouped[dataset].append(snapshot)
    return grouped


def main():
    parser = argparse.ArgumentParser(
        description="Keepoid: ZFS snapshot retention and pruning tool."
    )
    parser.add_argument(
        "--config",
        default="/etc/keepoid/keepoid.conf",
        help="Path to the configuration file.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print actions without executing them."
    )
    args = parser.parse_args()

    try:
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {args.config}")
        return

    global_start_time_str = config.get("startTime", "00:00")
    prune_after = parse_duration(config.get("pruneAfter"))
    path = config.get("path")
    skip_parent = config.get("skipParent", False)
    identifier = config.get("identifier")
    retention_policies = config.get("retention", [])

    if not path or not identifier:
        print("Error: 'path' and 'identifier' must be defined in the config.")
        return

    all_snapshots = get_snapshots(path, identifier, skip_parent)
    if not all_snapshots:
        print("No snapshots found to process.")
        return

    now = datetime.now()

    snapshots_by_dataset = group_snapshots_by_dataset(all_snapshots)

    all_snapshots_to_prune = []
    all_policy_kept_snapshots = set()
    all_prune_after_kept_snapshots = []

    for dataset_path, dataset_snapshots in snapshots_by_dataset.items():
        (
            snapshots_to_prune,
            policy_kept_snapshots,
            prune_after_kept_snapshots,
        ) = determine_snapshots_to_prune(
            dataset_snapshots,
            retention_policies,
            prune_after,
            global_start_time_str,
            now,
            dataset_path,
        )
        all_snapshots_to_prune.extend(snapshots_to_prune)
        all_policy_kept_snapshots.update(policy_kept_snapshots)
        all_prune_after_kept_snapshots.extend(prune_after_kept_snapshots)

    total_kept = len(all_policy_kept_snapshots) + len(
        all_prune_after_kept_snapshots
    )

    print(f"Found {len(all_snapshots)} snapshots across {len(snapshots_by_dataset)} datasets.")
    print(
        f"Keeping {total_kept} snapshots ({len(all_policy_kept_snapshots)} by policy, {len(all_prune_after_kept_snapshots)} by pruneAfter)."
    )
    print(f"Identified {len(all_snapshots_to_prune)} snapshots to prune.")

    for snapshot in all_snapshots_to_prune:
        destroy_snapshot(snapshot.name, dry_run=args.dry_run)

    if not all_snapshots_to_prune:
        print("No snapshots to prune.")


if __name__ == "__main__":
    main()
