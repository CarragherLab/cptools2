"""
Tests for cptools.check_for_failed
"""

from cptools2 import check_for_failed


def test_parse_h_rt():
    """cptools2.check_for_failed.Sweeper.parse_rt(runtime)"""
    test_runtime_str = "01:00:00"
    one_hour_ans = check_for_failed.Sweeper.parse_rt(test_runtime_str)
    assert one_hour_ans == 3600
    test_runtime_str2 = "00:00:60"
    one_min_ans = check_for_failed.Sweeper.parse_rt(test_runtime_str2)
    assert one_min_ans == 60
    test_runtime_str3 = "00:01:00"
    one_min_ans2 = check_for_failed.Sweeper.parse_rt(test_runtime_str3)
    assert one_min_ans2 == 60


def test_parse_vmem():
    """cptools2.check_for_failed.Sweeper.parse_vmem(vmem)"""
    assert check_for_failed.Sweeper.parse_vmem("12G") == 12
    assert check_for_failed.Sweeper.parse_vmem("1G") == 1


def test_sweeper():
    """cptools2.check_for_failed.Sweeper()"""
    pass