from functools import total_ordering
import json
from tabulate import tabulate
import argparse
import sys

parser = argparse.ArgumentParser(description='Parsing presto logs')
parser.add_argument('--raw', default=False, help='Time logs are raw sessionLogEntries json list')
parser.add_argument('--file', type=str, help='Input file')
args = parser.parse_args()

def load_json_file(args):
    if args.file is None:
        return json.load(sys.stdin)
    else:
        with open(args.file) as f:
            return json.load(f)


def get_session_log_entries(obj, taskid):
    def with_taskid(o):
        o["task"] = taskid
        return o

    return [with_taskid(o) for o in obj.get("sessionLogEntries", [])]


def populate_session_log_entries(sessionLogEntries, stages):
    for stage in stages:
        for task in stage.get("tasks", []):
            taskid = task["taskStatus"]["taskId"].split(".",2)[-1]
            sessionLogEntries.extend(get_session_log_entries(task, taskid))
        populate_session_log_entries(sessionLogEntries, stage.get("subStages", []))

def get_time_logs(args):
    presto = load_json_file(args)
    if args.raw:
        return presto
    sessionLogEntries = get_session_log_entries(presto, "coordinator")
    stage = presto.get("outputStage", None)
    if stage is not None:
        populate_session_log_entries(sessionLogEntries, [stage])
    return sessionLogEntries

def get_split_number(split):
    return int(split.split('-')[1])

split_progress_marks = {
    'offering split Split': 'offered',
    'running split Split': 'running',
    'pausing split Split': 'pausing',
    'split is finished Split': 'finished'
}

aql_progress_marks = {
    'Aql Issue Start': 'aql_start',
    'Aql Issue End': 'aql_end',
    'Ares getNextPage start': 'page_start',
    'Ares getNextPage end': 'page_end'
}
split_progress = {}
aql_progress = {}
def record_split_progress(time_log):
    message = time_log.message
    nanos = time_log.nanos
    for k, v in split_progress_marks.items():
        if message.startswith(k):
            split = get_split_number(message[len(k):].strip() if v[1] else time_log.thread_name)
            split_progress[split] = split_progress.get(split, {})
            split_progress[split][v[0]] = nanos
            break
    for k, v in aql_progress_marks.items():
        if message.startswith(k):
            aql_idx = -1
            try:
                aql_idx = int(message[len(k):].strip())
            except ValueError:
                pass
            if aql_idx >= 0:
                split = get_split_number(time_log.thread_name)
                aql_progress[aql_idx] = aql_progress.get(aql_idx, {})
                aql_progress[aql_idx][v[0]] = nanos
                split_progress[split] = split_progress.get(split, {})
                if 'ares_start' in split_progress[split]:
                    split_progress[split]['ares_start'] = min(split_progress[split]['ares_start'], nanos)
                else:
                    split_progress[split]['ares_start'] = nanos
                if 'ares_end' in split_progress[split]:
                    split_progress[split]['ares_end'] = max(split_progress[split]['ares_end'], nanos)
                else:
                    split_progress[split]['ares_end'] = nanos
                break

def print_split_progress_duration(start, end, table):
    durations = []
    for k, v in table.items():
        if start in v and end in v:
            durations.append((k, v[end] - v[start]))
    durations.sort(key = lambda item: item[1])
    durations_ms = [(k, int(v/1e6)) for k, v in durations]
    print(f'Split duration between {start} to {end}')
    print(tabulate(durations_ms, headers=["split_number", f'{start}-{end}'], tablefmt="pipe"))

def prune_to_only_ares_splits():
    keys_to_delete = []
    for k, v in split_progress.items():
        if not any(('ares' in x for x in v.keys())):
            keys_to_delete.append(k)

    for k in keys_to_delete:
        del split_progress[k]

@total_ordering
class Measure(object):
    def __init__(self, entry):
        self.message = entry["message"]
        self.nanos = entry["nanos"]
        self.task = entry["task"]
        self.thread_name = entry["threadName"]

    def _internal(self):
        return (self.nanos, self.message, self.thread_name) # no task, since there can be double logging

    def __eq__(self, other):
        return (self._internal() == other._internal())

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return self._internal() < other._internal()

    def __repr__(self):
        return str(self._internal())

    def __hash__(self):
        return hash(self._internal())

proscribed = []
prev_time = None
deltas = []
prev_message = None
entries = []
headers = ["delta_ms", "absolute_ms", "activity", "thread", "task"]
first_time = None
time_logs = sorted(set([Measure(x) for x in get_time_logs(args)]))
for time_log in time_logs:
    message = time_log.message
    nanos = time_log.nanos
    record_split_progress(time_log)
    if first_time is None:
        first_time = nanos
    if prev_time is not None:
        delta_time = round((nanos - prev_time)/1e6, 2)
        first_time_delta = round((nanos - first_time)/1e6, 2)
        delta_name = ' --BW-- '.join([prev_message, message])
        deltas.append((delta_time, delta_name, first_time_delta))
        entries.append([delta_time, first_time_delta, message[0:95], time_log.thread_name[0:40], time_log.task])

    prev_message = message
    prev_time = nanos

prune_to_only_ares_splits()
print_split_progress_duration('running', 'finished', split_progress)
print_split_progress_duration('ares_start', 'ares_end', split_progress)
print_split_progress_duration('offered', 'running', split_progress)
print_split_progress_duration('running', 'ares_start', split_progress)
print_split_progress_duration('ares_end', 'finished', split_progress)
print_split_progress_duration('pausing', 'finished', split_progress)
print_split_progress_duration('aql_start', 'aql_end', aql_progress)
print_split_progress_duration('page_start', 'page_end', aql_progress)
print(tabulate(entries, headers=headers, tablefmt="pipe"))

