from datetime import datetime, timezone

from test_reconcile_sources import _load_module


def test_new_source_does_not_backfill_before_actual_first_public_init():
    module = _load_module()
    policies = {p.cycle: p for p in module.POLICIES if p.source == "ifs-hres-0p1"}
    assert set(policies) == {0, 12}
    now = datetime(2026, 10, 21, 12, tzinfo=timezone.utc)
    assert module.candidate_dates(now, policies[0]) == ("20261021",)
    assert module.candidate_dates(now, policies[12]) == ("20261020",)
    assert all(policy.expected_steps == module.IFS_STEPS for policy in policies.values())


def test_original_025_operational_policy_is_unchanged():
    module = _load_module()
    old = [p for p in module.POLICIES if p.source == "ifs-hres"]
    assert [(p.cycle, p.publish_lag_hours, p.lookback_days, p.first_init) for p in old] == [
        (12, 8, 2, None), (0, 8, 2, None)]
