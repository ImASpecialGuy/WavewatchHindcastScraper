import threading
import multiprocessing
import argparse


def printDates(start_month, number_of_months):
    for month in range(start_month, start_month + number_of_months):
        # this is painful because January is month 1 and not month 0
        print(str((month-1)//12) + "{:02d}".format((month-1) % 12+1) + '\n', end='')


def clamp_month(month):
    return max(1, min(month, 12))


def clamp_year(year):
    return max(1979, min(year, 2009))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start-date", type=str,
                        help="Inclusive start date in format 'MM-YYYY' (min: 01-1979; max: 12-2009)", default="01-1979")
    parser.add_argument("-e", "--end-date", type=str,
                        help="Inclusive end date in format 'MM-YYYY' (min: 01-1979; max: 12-2009)", default="12-2009")
    args = parser.parse_args()
    start_month, start_year = map(int, args.start_date.split("-"))
    end_month, end_year = map(int, args.end_date.split("-"))

    # Catch invalid dates by clamping them to the min and max values
    start_month, end_month = map(clamp_month, (start_month, end_month))
    start_year, end_year = map(clamp_year, (start_year, end_year))

    # Add one because if end_year == start_year and end_month == start_month, we should do 1 month
    number_of_months = end_year * 12 + end_month - start_year * 12 - start_month + 1

    # Putting everything in start_month is easier for threading distribution
    start_month += start_year * 12

    # Divide work amongst threads and call printDates on those threads
    num_cores = multiprocessing.cpu_count()
    months_per_thread = number_of_months // num_cores
    threads_workload = [months_per_thread for i in range(num_cores)]
    remaining_months = number_of_months % num_cores
    threads = []
    for i in range(len(threads_workload)):
        # evenly divide the remaining months
        if remaining_months > 0:
            threads_workload[i] += 1
            remaining_months -= 1

        thread = threading.Thread(target=printDates, args=(start_month, threads_workload[i],))
        thread.start()
        threads.append(thread)
        start_month += threads_workload[i]

    # Wait for the threads to finish
    for thread in threads:
        thread.join()
