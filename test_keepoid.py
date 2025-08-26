import pytest
from datetime import datetime, timedelta
import random
import re
from pathlib import Path
from keepoid import parse_duration, determine_snapshots_to_prune, Snapshot, group_snapshots_by_dataset

now = datetime(2023, 1, 31, 0, 0, 0)
global_start_time_str = "00:00"


def test_parse_duration():
    assert parse_duration("30s") == timedelta(seconds=30)
    assert parse_duration("10m") == timedelta(minutes=10)
    assert parse_duration("2h") == timedelta(hours=2)
    assert parse_duration("7d") == timedelta(days=7)
    with pytest.raises(ValueError):
        parse_duration("1y")


def filter_snapshots(snapshots, filter_snapshots):
    return filter(lambda x: x not in filter_snapshots, snapshots)


@pytest.fixture
def sample_snapshots():
    # Create hourly snapshots for the last 48 hours
    # The first snapshot will start at 00:00, matching global_start_time_str
    snapshots = []
    for i in range(48):
        snap_time = now - timedelta(hours=i)
        snapshots.append(
            Snapshot(f"pool/data@autosnap_{snap_time.isoformat()}", snap_time)
        )
    return snapshots


@pytest.fixture
def randomized_snapshots():
    # Create hourly snapshots for the last 48 hours with random second offsets
    snapshots = []
    for i in range(48):
        snap_time = now - timedelta(hours=i)
        snap_time += timedelta(seconds=random.randint(1, 59))
        snapshots.append(
            Snapshot(f"pool/data@autosnap_{snap_time.isoformat()}", snap_time)
        )
    return snapshots


def test_hourly_policy(sample_snapshots):
    # Keep 24 hourly
    retention_policies = [
        {"interval": "1h", "count": 24},
    ]
    prune_after = parse_duration("0m")  # Prune immediately

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        sample_snapshots,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data",
    )

    # We have 48 snapshots.
    # We keep 24 hourly snapshots (+1)
    expected_policy_kept = 25
    assert len(policy_kept) == expected_policy_kept

    # Since pruneAfter is very short, no snapshots should be kept by it.
    assert len(prune_after_kept) == 0

    # 48 total - 25 kept = 23 to prune
    assert len(snapshots_to_prune) == (len(sample_snapshots) - expected_policy_kept)


def test_randomized_seconds_policy(randomized_snapshots):
    # Keep 5 + 1 hourly snapshots, even with random seconds
    retention_policies = [
        {"interval": "1h", "count": 5},
    ]
    prune_after = parse_duration("0m")

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        randomized_snapshots,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data",
    )

    # We expect 5 + 1 snapshots to be kept by the policy.
    # The logic should find the best match for each of the 5 hourly intervals.
    assert len(policy_kept) == 6

    # Since pruneAfter is 0m, nothing extra is kept.
    assert len(prune_after_kept) == 0

    # 48 total - 5 kept = 43 to prune
    assert len(snapshots_to_prune) == 42


def test_daily_policy(sample_snapshots):
    # Keep 7 daily
    retention_policies = [
        {"interval": "1d", "count": 7},
    ]
    prune_after = parse_duration("0m")  # Prune immediately

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        sample_snapshots,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data",
    )

    # We have 48 hourly snapshots
    # Hours 0, 24, and 47 are expected to be kept
    # 47 is kept because 48 does not exist, and it is the next best option
    expected_policy_kept = {
        sample_snapshots[0],
        sample_snapshots[24],
        sample_snapshots[47],
    }
    assert policy_kept == expected_policy_kept

    # Since pruneAfter is very short, no snapshots should be kept by it.
    assert prune_after_kept == []

    # 48 total - 25 kept = 23 to prune
    expected_policy_prune = list(
        filter_snapshots(sample_snapshots, expected_policy_kept)
    )
    assert snapshots_to_prune == expected_policy_prune


def test_2day_policy(sample_snapshots):
    # Keep 7 daily
    retention_policies = [
        {"interval": "2d", "count": 7},
    ]
    prune_after = parse_duration("0m")  # Prune immediately

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        sample_snapshots,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data",
    )

    # We have 48 hourly snapshots
    # Hours 0 and 47 are expected to be kept
    # 47 is kept because 48 does not exist, and it is the next best option
    expected_policy_kept = {sample_snapshots[0], sample_snapshots[47]}
    assert policy_kept == expected_policy_kept

    # Since pruneAfter is very short, no snapshots should be kept by it.
    assert prune_after_kept == []

    # 48 total - 25 kept = 23 to prune
    expected_policy_prune = list(
        filter_snapshots(sample_snapshots, expected_policy_kept)
    )
    assert snapshots_to_prune == expected_policy_prune


def test_merged_retention_logic(sample_snapshots):
    # Keep 24 hourly, 7 daily
    retention_policies = [
        {"interval": "1h", "count": 24},
        {"interval": "1d", "count": 7},
    ]
    prune_after = parse_duration("0m")  # Prune immediately if not kept

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        sample_snapshots,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data",
    )

    # We have 48 snapshots.
    # We keep 24 hourly snapshots (+1), hours 0-25
    # The daily policy will select 3 snapshots (hour 0, hour 24, and hour 47).
    # 3 daily snapshots are already covered under hourly, so 26 are expexted.
    # The use of set() below will remove the duplicates for us.
    expected_hourly_kept = sample_snapshots[:25]
    expected_daily_kept = [
        sample_snapshots[0],
        sample_snapshots[24],
        sample_snapshots[47],
    ]
    expected_policy_kept = set(expected_hourly_kept + expected_daily_kept)
    assert policy_kept == expected_policy_kept

    # Since pruneAfter is very short, no snapshots should be kept by it.
    assert prune_after_kept == []

    # 48 total - 26 kept = 22 to prune
    expected_policy_prune = list(
        filter_snapshots(sample_snapshots, expected_policy_kept)
    )
    assert snapshots_to_prune == expected_policy_prune


def test_prune_after(sample_snapshots):
    retention_policies = [
        {"interval": "1h", "count": 1},  # Keep only the latest snapshot (+1)
    ]
    # Set prune_after to 36 hours. Snapshots older than this can be pruned.
    prune_after = parse_duration("36h")

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        sample_snapshots,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data",
    )

    # Hour 0 and hour 1
    expected_policy_kept = {sample_snapshots[0], sample_snapshots[1]}
    assert policy_kept == expected_policy_kept

    # Snapshots created in the last 36 hours are kept by pruneAfter.
    # Since the above 2 snapshots are are kept by policy,
    # they should not be defined in `prune_after_kept`
    # 35 snapshots are expected to be kept by pruneAfter
    assert prune_after_kept == list(
        filter_snapshots(sample_snapshots[:37], expected_policy_kept)
    )

    # Snapshots to prune are those not kept AND older than prune_after (36h)
    # Snapshots 37-47 are eligible for pruning. (12 snapshots)
    # Snapshot 36 is not expected to be pruned because it does not exceed 36 hours ago
    assert snapshots_to_prune == sample_snapshots[37:]


@pytest.fixture
def multi_dataset_snapshots():
    snapshots = []
    # Create hourly snapshots for the last 48 hours for two datasets
    for i in range(48):
        snap_time = now - timedelta(hours=i)
        snapshots.append(
            Snapshot(f"pool/data/A@autosnap_{snap_time.isoformat()}", snap_time)
        )
        snapshots.append(
            Snapshot(f"pool/data/B@autosnap_{snap_time.isoformat()}", snap_time)
        )
    return snapshots


def test_multi_dataset_policy(multi_dataset_snapshots):
    # Keep 24 hourly for dataset A, 12 hourly for dataset B, 5 for others (default)
    retention_policies = [
        {"interval": "1h", "count": 24, "path": "pool/data/A"},
        {"interval": "1h", "count": 12, "path": "pool/data/B"},
    ]
    prune_after = parse_duration("0m")

    snapshots_a = [s for s in multi_dataset_snapshots if s.name.startswith("pool/data/A")]
    snapshots_b = [s for s in multi_dataset_snapshots if s.name.startswith("pool/data/B")]

    prune_a, kept_a, pa_kept_a = determine_snapshots_to_prune(
        snapshots_a,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data/A",
    )
    prune_b, kept_b, pa_kept_b = determine_snapshots_to_prune(
        snapshots_b,
        retention_policies,
        prune_after,
        global_start_time_str,
        now,
        "pool/data/B",
    )

    # Dataset A: 48 snapshots, 25 kept by policy (24+1)
    assert len(kept_a) == 25
    assert len(prune_a) == 23
    assert len(pa_kept_a) == 0

    # Dataset B: 48 snapshots, 13 kept by policy (12+1)
    assert len(kept_b) == 13
    assert len(prune_b) == 35
    assert len(pa_kept_b) == 0


def parse_real_snapshot(snapshot_line):
    """Parse a real snapshot line into a Snapshot object"""
    snapshot_line = snapshot_line.strip()
    if not snapshot_line:
        return None

    # Extract dataset and snapshot name
    dataset, snapshot_name = snapshot_line.split('@', 1)

    # Parse different snapshot types
    timestamp = None

    if 'autosnap_' in snapshot_name:
        # Parse autosnap format: autosnap_2025-08-24_22:00:03_frequently
        match = re.search(r'autosnap_(\d{4}-\d{2}-\d{2})_(\d{2}:\d{2}:\d{2})_', snapshot_name)
        if match:
            date_part = match.group(1)
            time_part = match.group(2)
            try:
                timestamp = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
    elif 'syncoid_' in snapshot_name:
        # Parse syncoid format: syncoid_2025-08-25:22:15:03-GMT-04:00
        match = re.search(r'syncoid_(\d{4}-\d{2}-\d{2}):(\d{2}:\d{2}:\d{2})', snapshot_name)
        if match:
            date_part = match.group(1)
            time_part = match.group(2)
            try:
                timestamp = datetime.strptime(f"{date_part} {time_part}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

    if timestamp is None:
        # Fallback timestamp for unparseable snapshots
        timestamp = datetime(2025, 8, 25, 12, 0, 0)

    return Snapshot(snapshot_line, timestamp)


@pytest.fixture
def real_snapshots():
    """Load real snapshots from test-snapshots.txt"""
    test_file = Path(__file__).parent / "test-snapshots.txt"
    snapshots = []

    with open(test_file, 'r') as f:
        for line in f:
            snapshot = parse_real_snapshot(line)
            if snapshot:
                snapshots.append(snapshot)

    return snapshots


def test_real_snapshot_parsing():
    """Test that we can parse real snapshot names correctly"""
    # Test autosnap parsing
    autosnap_line = "backup/dataset/3cd362dbcefa@autosnap_2025-08-24_22:00:03_frequently"
    snapshot = parse_real_snapshot(autosnap_line)

    assert snapshot is not None
    assert snapshot.name == autosnap_line
    assert snapshot.creation_time == datetime(2025, 8, 24, 22, 0, 3)

    # Test syncoid parsing
    syncoid_line = "backup/dataset/3cd362dbcefa@syncoid_2025-08-25:22:15:03-GMT-04:00"
    snapshot = parse_real_snapshot(syncoid_line)

    assert snapshot is not None
    assert snapshot.name == syncoid_line
    assert snapshot.creation_time == datetime(2025, 8, 25, 22, 15, 3)


def test_real_snapshots_grouping(real_snapshots):
    """Test grouping real snapshots by dataset"""
    grouped = group_snapshots_by_dataset(real_snapshots)

    # Should have multiple datasets
    assert len(grouped) > 1

    # Check that we have the expected dataset pattern
    dataset_names = list(grouped.keys())
    assert all(name.startswith("backup/dataset/") for name in dataset_names)

    # Each dataset should have multiple snapshots
    for dataset, snaps in grouped.items():
        assert len(snaps) > 1
        # Should have different types of snapshots per dataset
        snapshot_types = set()
        for snap in snaps:
            if 'autosnap_' in snap.name:
                if '_frequently' in snap.name:
                    snapshot_types.add('frequently')
                elif '_hourly' in snap.name:
                    snapshot_types.add('hourly')
            elif 'syncoid_' in snap.name:
                snapshot_types.add('syncoid')

        assert len(snapshot_types) > 1, f"Dataset {dataset} should have multiple snapshot types"


def test_real_snapshots_pruning_policy(real_snapshots):
    """Test pruning policy on real snapshot data"""
    # Group snapshots by dataset
    grouped = group_snapshots_by_dataset(real_snapshots)

    # Test on first dataset
    first_dataset = list(grouped.keys())[0]
    dataset_snapshots = grouped[first_dataset]

    # Define a realistic retention policy
    retention_policies = [
        {"interval": "1h", "count": 24},  # Keep 24 hourly
    ]
    prune_after = parse_duration("1h")  # Keep everything within 30 days

    # Use a current_time that's just after the newest snapshot
    current_time = datetime(2025, 8, 26, 2, 5, 0)

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        dataset_snapshots,
        retention_policies,
        prune_after,
        "00:00",
        current_time,
        first_dataset,
    )

    assert len(policy_kept) == 25
    assert len(snapshots_to_prune) == 4
    assert len(prune_after_kept) == 0


def test_real_snapshots_time_ordering(real_snapshots):
    """Test that real snapshots are correctly time-ordered"""
    # Group by dataset
    grouped = group_snapshots_by_dataset(real_snapshots)

    for dataset, snaps in grouped.items():
        # Filter to autosnap snapshots only for consistent comparison
        autosnap_snaps = [s for s in snaps if 'autosnap_' in s.name]

        if len(autosnap_snaps) > 1:
            # Sort by creation time
            sorted_snaps = sorted(autosnap_snaps, key=lambda x: x.creation_time)

            # Verify ordering
            for i in range(1, len(sorted_snaps)):
                assert sorted_snaps[i-1].creation_time <= sorted_snaps[i].creation_time, \
                    f"Snapshots should be time-ordered in dataset {dataset}"
