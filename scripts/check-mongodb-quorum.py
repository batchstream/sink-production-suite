"""Require real election, loss-of-majority rejection and reconciled recovery."""

import json
import math
import pathlib
import sys


def check(directory):
    report = json.loads((directory / "load.json").read_text())
    pause = int((directory / "pause-ns.txt").read_text())
    resume = int((directory / "resume-ns.txt").read_text())
    first = math.ceil((pause - report["started_unix_ns"]) / 1e9) + 3
    last = math.floor((resume - report["started_unix_ns"]) / 1e9) - 1
    outage = [row for row in report["timeline"] if first <= row["second"] <= last]
    after = [row for row in report["timeline"] if row["second"] > last + 10]
    assert report["verified"], "acknowledged writes did not reconcile"
    assert report["successful_operations"] >= 1000, "insufficient successful load"
    assert last - first >= 4 and outage, "no bounded majority-loss window"
    assert sum(row["successful_rpcs"] for row in outage) == 0, "write acknowledged without a majority"
    assert sum(row["failed_rpcs"] for row in outage) > 0, "majority loss did not reject writes"
    assert sum(row["successful_rpcs"] for row in after) > 100, "no recovery after quorum restoration"
    print("PASS primary election, majority-loss rejection and reconciled recovery")
    print(json.dumps({key: report[key] for key in ["successful_operations", "errors", "verified", "reconciled_unacknowledged"]}))


if __name__ == "__main__":
    check(pathlib.Path(sys.argv[1]))
