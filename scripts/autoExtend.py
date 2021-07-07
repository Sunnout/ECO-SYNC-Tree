#!/usr/bin/env python3

from datetime import datetime, timedelta
import time
import os
import subprocess
import argparse
import json

min_time_to_extend_mins = 8
min_time_to_extend = timedelta(minutes=min_time_to_extend_mins)


def parse_args():
    parser = argparse.ArgumentParser()
    optional = parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    required.add_argument("--nr_extensions", required=True,   metavar='nr_extensions',
                          type=int,   help="Number of times jobs can be extended")

    required.add_argument("--user", required=True,   metavar='user',
                          type=str,   help="the user issuing the job")

    required.add_argument("--disable_at_forbidden_hours", required=True,   metavar='disable_at_forbidden_hours',
                          type=bool,   help="if the script should exit at hours where the job would be exceed usage (8:00 and 18:00)")
    parser._action_groups.append(optional)
    args = parser.parse_args()
    return args


def run_cmd_with_try(cmd, env=dict(os.environ), stdout=subprocess.DEVNULL):
    print(f"Running | {cmd} | LOCAL")
    cp = subprocess.run(cmd, shell=True, stdout=stdout, env=env)
    if cp.stderr is not None:
        raise Exception(cp.stderr)


def exec_cmd_with_output(cmd):
    (status, out) = subprocess.getstatusoutput(cmd)
    # print(f"status: {status}, out: {out}")
    if status != 0:
        print(out)
        exit(1)
    return out


def exit_invalid_hour():
    time_now = datetime.now()
    hour_now = time_now.hour
    print(f"Err: Cannot extend jobs at hour: {hour_now}")
    exit(1)


def main():
    args = parse_args()
    get_sched_jobs_cmd = f"oarstat -fJ -u {args.user}"
    extensions = {}
    print()
    while True:
        try:
            time_now = datetime.now()
            if args.disable_at_forbidden_hours:
                hour_now = time_now.hour
                minute_now = time_now.minute
                print("hour_now:", hour_now)
                if hour_now == 8 or hour_now == 18:
                    exit_invalid_hour()
                if hour_now == 7 or hour_now == 17:
                    if minute_now >= 50:
                        exit_invalid_hour()

            running_jobs_str = exec_cmd_with_output(get_sched_jobs_cmd)
            if running_jobs_str == "":
                print(f"err: no jobs for user {args.user}")
                exit(1)

            running_jobs = json.loads(running_jobs_str)

            if len(running_jobs) == 0:
                print(f"err: no jobs for user {args.user}")
                exit(1)

            for job_id in running_jobs:
                if job_id not in extensions:
                    extensions[job_id] = 0
                if running_jobs[job_id]["state"] == "Running":
                    walltime = timedelta(seconds=int(
                        running_jobs[job_id]["walltime"]))
                    start_time = datetime.fromtimestamp(
                        int(running_jobs[job_id]["startTime"]))

                    end_time = start_time + walltime
                    time_to_finish = end_time - datetime.now()

                    print("---------------------------------------------------")
                    print("job_id\t\t\t", job_id)
                    print("walltime\t\t", walltime)
                    print("start_time\t\t", start_time)
                    print("end_time\t\t", end_time)
                    print("time_to_finish\t\t", time_to_finish)

                    if time_to_finish < min_time_to_extend:
                        if extensions[job_id] <= args.nr_extensions:
                            extensions[job_id] += 1
                            print(f"extending job {job_id}")
                            extend_cmd = f"oarwalltime {job_id} +1"
                            extend_out = exec_cmd_with_output(extend_cmd)
                            print(f"extend command output: {extend_out}")
                        else:
                            print(
                                f"allowing job {job_id} to end because it has been extended too many times")
                    else:
                        print(
                            f"not extending {job_id} because there is more than {min_time_to_extend} remaining in job")
                    print("---------------------------------------------------")
                    print()
                else:
                    print(f"Not extending job {job_id} as it is not running")

            print("Checking again in 2 minutes...")
            time.sleep(120)
        except Exception as e:
            print(f"Exception: {e}")
            exit(1)


if __name__ == "__main__":
    main()
