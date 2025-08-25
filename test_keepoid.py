import pytest
from datetime import datetime, timedelta
from keepoid import parse_duration, determine_snapshots_to_prune, Snapshot

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


def test_hourly_policy(sample_snapshots):
    # Keep 24 hourly
    retention_policies = [
        {"interval": "1h", "count": 24},
    ]
    prune_after = parse_duration("0m")  # Prune immediately

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        sample_snapshots, retention_policies, prune_after, global_start_time_str, now
    )

    # We have 48 snapshots.
    # We keep 24 hourly snapshots (+1)
    expected_policy_kept = 25
    assert len(policy_kept) == expected_policy_kept

    # Since pruneAfter is very short, no snapshots should be kept by it.
    assert len(prune_after_kept) == 0

    # 48 total - 25 kept = 23 to prune
    assert len(snapshots_to_prune) == (len(sample_snapshots) - expected_policy_kept)


def test_daily_policy(sample_snapshots):
    # Keep 7 daily
    retention_policies = [
        {"interval": "1d", "count": 7},
    ]
    prune_after = parse_duration("0m")  # Prune immediately

    snapshots_to_prune, policy_kept, prune_after_kept = determine_snapshots_to_prune(
        sample_snapshots, retention_policies, prune_after, global_start_time_str, now
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
        sample_snapshots, retention_policies, prune_after, global_start_time_str, now
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
        sample_snapshots, retention_policies, prune_after, global_start_time_str, now
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
        sample_snapshots, retention_policies, prune_after, global_start_time_str, now
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
