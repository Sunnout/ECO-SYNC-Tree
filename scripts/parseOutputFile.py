import datetime as dt


def parse_output(exp_path):
    run_start_time = -1
    first_dead_time = -1
    last_start_time = -1
    catastrophe_start_time = -1
    churn_start_time = -1
    churn_end_time = -1

    # PROCESS OUTPUT FILE
    output_file = open(exp_path + "/output.txt", "r")
    for i in output_file:
        line = i.split(" ", 1)

        if line[0] == "FIRST_NODE":
            run_start_time = dt.datetime.strptime(line[1].strip(), '%a %d %b %Y %I:%M:%S %p %Z').timestamp()

        elif line[0] == "FIRST_DEAD":
            if run_start_time == -1:
                print("Error in run start time")
                exit()
            first_dead_time = dt.datetime.strptime(line[1].strip(), '%a %d %b %Y %I:%M:%S %p %Z').timestamp() - run_start_time

        elif line[0] == "LAST_NODE":
            if run_start_time == -1:
                print("Error in run start time")
                exit()
            last_start_time = dt.datetime.strptime(line[1].strip(), '%a %d %b %Y %I:%M:%S %p %Z').timestamp() - run_start_time

        elif line[0] == "START_CATASTROPHE":
            if run_start_time == -1:
                print("Error in run start time")
                exit()
            catastrophe_start_time = dt.datetime.strptime(line[1].strip(),
                                                          '%a %d %b %Y %I:%M:%S %p %Z').timestamp() - run_start_time

        elif line[0] == "START_CHURN":
            if run_start_time == -1:
                print("Error in run start time")
                exit()
            churn_start_time = dt.datetime.strptime(line[1].strip(), '%a %d %b %Y %I:%M:%S %p %Z').timestamp() - run_start_time

        elif line[0] == "END_CHURN":
            if run_start_time == -1:
                print("Error in run start time")
                exit()
            churn_end_time = dt.datetime.strptime(line[1].strip(), '%a %d %b %Y %I:%M:%S %p %Z').timestamp() - run_start_time

    return run_start_time, first_dead_time, last_start_time, catastrophe_start_time, churn_start_time, churn_end_time
