import subprocess
import time
import os
import re
import shlex
import datetime
import psutil
import shutil

OPERATING_SYSTEM = "linux"

def output_process_string(bash_args):
    sanitized_args = [shlex.quote(arg) for arg in bash_args]
    return " ".join(sanitized_args)

def current_utc_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def log_timestamp():
    current_utc_timestamp() + ": " + output_process_string([])

def initial_python_args():
    if OPERATING_SYSTEM == "linux":
        return ["python3"]
    if OPERATING_SYSTEM == "windows":
        return ["py", "-3"]


def get_process_pids(process):
    pid = process.pid
    pids = [pid]
    for child in psutil.Process(pid).children(recursive=True):
        pids.append(child.pid)
    return pids

def run_afs_server_subprocess(server_id, address, server_ids, addresses, working_directory, log_dir, primary=False, prepopulated_files = ["input_dataset_001.txt", "input_dataset_002.txt", "input_dataset_003.txt"], path_to_subprocess="cmd/server/main.go"):
    os.makedirs(log_dir, exist_ok=True)
    replica_path = os.path.join(working_directory, str(server_id))
    os.makedirs(replica_path, exist_ok=True)
    #Copy prepopulated files from main data to replica directory
    for file in prepopulated_files:
        shutil.copy(os.path.join(working_directory, file), os.path.join(replica_path, file))

    log_file_path = os.path.join(log_dir, "server" + str(server_id) + ".log")

    bash_args = [
        "go",
        "run",
        path_to_subprocess,
        "-id", str(server_id),
        "-addr", address,
        "-replicas", ",".join(addresses),
        "-primary", "true" if primary else "false",
        "-working", replica_path
    ]

    all_process_ids = None
    with open(log_file_path, "a") as logfile:
        logfile.write("Running " + output_process_string(bash_args)+"\n")
        process = subprocess.Popen(
            bash_args,
            stdout=logfile,
            stderr=logfile,
            shell=False
        )
        all_process_ids = get_process_pids(process)

    return all_process_ids

    

def run_coordinator_subprocess(log_dir, server_addresses, id="coordinator", path_to_subprocess="pkg/afs/coordinator.py"):
    bash_args = ["py", "-3"]
    bash_args.extend([
        "-u", path_to_subprocess,
        id,
        ",".join(server_addresses),
    ])
    log_file_path = os.path.join(log_dir, "coordinator" + ".log")

    all_pids = None

    with open(log_file_path, "w") as logfile:
        process = subprocess.Popen(
            bash_args,
            stdout=logfile,
            stderr=logfile
        )
        all_pids = get_process_pids(process)
    
    return all_pids

def run_worker_subprocess(worker_id, server_addresses, log_dir, fermats_number=5, path_to_subprocess="pkg/afs/worker.py"):
    bash_args = ["py", "-3"]
    bash_args.extend([
        "-u", path_to_subprocess,
        str(worker_id),
        ",".join(server_addresses),
        str(fermats_number)
    ])
    log_file_path = os.path.join(log_dir, "worker" + str(worker_id) +  ".log")
    all_pids = None
    with open(log_file_path, "w") as logfile:
        process = subprocess.Popen(
            bash_args,
            stdout=logfile,
            stderr=logfile
        )
        all_pids = get_process_pids(process)

    return all_pids

def run_client_subprocess(client_id, server_addresses, log_dir, max_retries=3, retry_delay=1, path_to_subprocess="pkg/afs/afsclient.py"):
    bash_args = ["py", "-3"]
    bash_args.extend([
        "-u", path_to_subprocess,
        str(client_id),
        ",".join(server_addresses),
        str(max_retries),
        str(retry_delay)
    ])
    log_file_path = os.path.join(log_dir, "afsclient" + str(client_id) +  ".log")

    all_pids = None
    with open(log_file_path, "w") as logfile:
        process = subprocess.Popen(
            bash_args,
            stdout=logfile,
            stderr=logfile
        )
        all_pids = get_process_pids(process)
    
    return all_pids



#A helper to create addresses for multiple servers from an initial address
def build_local_address_strings(num_replicas, initial_address):
    #Get port number from address
    initial_port = None
    split_address = initial_address.split(":")
    if len(split_address) > 1:
        port = split_address[-1]
        if port.isdigit(): 
            initial_port = int(port)
        else:
            raise ValueError("initial_address does not end in port number")
    addresses = []
    #Build address from string
    for i in range(num_replicas):
        address = split_address[:-1]
        address.append(str(initial_port+i))
        address = ":".join(address)
        addresses.append(address)
    return addresses

#A helper to check the log
def check_log(log_dir, log_name, search_term, error_callback=None):
    try:
        f = open(os.path.join(log_dir,log_name), "r")
    except: 
        if error_callback:
            try:
                error_callback()
            except:
                raise ValueError("Error callback must be callable")
    else:
        file_str = f.read()
        search_results = re.findall(search_term, file_str)
        f.close()
        return search_results

#Submethod for AFS
def run_afs_system(num_replicas, initial_address, afs_directory, log_dir, primary_server=0):
    addresses = build_local_address_strings(num_replicas, initial_address)

    afs_processes = []

    for i, address in enumerate(addresses):
        afs_processes.append(
            run_afs_server_subprocess(
                i, 
                address, 
                range(num_replicas), 
                addresses, 
                afs_directory, 
                log_dir, 
                "true" if i == primary_server else "false"
            )
        )
    return afs_processes


def run_system(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=5, primary_server=0):
    addresses = build_local_address_strings(num_replicas, initial_address)

    subprocesses = {
        "afs": [],
        "coordinator": None,
        "worker": []
    }

    for i, address in enumerate(addresses):
        subprocesses["afs"].append(
            run_afs_server_subprocess(
                i, 
                address, 
                range(num_replicas), 
                addresses, 
                afs_directory, 
                log_dir, 
                "true" if i == primary_server else "false"
            )
        )

    subprocesses["coordinator"] = run_coordinator_subprocess(log_dir, addresses)

    time.sleep(3)

    for i in range(num_workers):
        subprocesses["worker"].append(
            run_worker_subprocess(i, addresses, log_dir, fermats_number=fermats_number)
        )
    return subprocesses
    
def quit_system(system_process_obj):
    for key in system_process_obj.keys():
        value = system_process_obj[key]
        if len(value) > 0 and type(value[0]) == list:
            for proc_list in value:
                for proc in proc_list:
                    psutil.Process(proc).kill()
        elif len(value) > 0 and type(value[0]) == int:
            for proc in value:
                psutil.Process(proc).kill()



#Basic worker snapshot test
def worker_snapshots_test(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=5):
    processes = run_system(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=fermats_number)

    print("Waiting for 45 seconds to populate snapshots")
    time.sleep(45)
    quit_system(processes)

    #Check snapshots directory for snapshots. 
    print("Snapshots content")
    directory_contents = os.listdir(snapshots_dir)
    if len(directory_contents) > 0:
        print(directory_contents)
    else:
        print("No snapshots created")


#Basic worker failure test
def worker_failure_test(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=5):
    processes = run_system(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=5)
    addresses = build_local_address_strings(num_replicas, initial_address)

    print("Wait for 15 seconds for Worker 0 to begin processing")
    time.sleep(15)
    print("Now we kill Worker 0")
    processes["worker"][0].kill()
    
    print("3 second wait, then restarting Worker 0")
    time.sleep(3)
    
    processes["worker"][0] = run_worker_subprocess(0, addresses, log_dir, fermats_number=fermats_number)
    
    print("attempted Worker 0 restart, waiting for 10 seconds then returning results")
    time.sleep(10)
    quit_system(processes)


    #Check criteria 1: if worker 0 recovered from being shut down
    model_registered = False
    register_search = check_log(log_dir, "worker0-restart.log", "registered")
    if register_search and len(register_search) >= 1:
        model_registered = True
    
    if model_registered:
        print("Criteria 1: Worker 0 successfully recovered")
    else:
        print("Criteria 1: Worker 0 failed to recover")


    #Check criteria 2: if worker 1 picked up work from worker 0
    completed_tasks = None
    def error_func():
        raise Exception("Error while checking criteria 2: Worker 1 log not accessed properly")

    completed_tasks = len(check_log(log_dir, "worker1.log", "Complete", error_callback= error_func))

    print("Criteria 2 " + ("SUCCESS" if completed_tasks > 0 else "FAILURE") + ": Worker 1 (expected backup) completed " + str(completed_tasks) + " tasks")
    

#Basic afs primary failure test
def afs_primary_failure_test(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=5):
    processes = run_system(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=fermats_number)


    print("Running for 15 seconds before killing primary server")
    time.sleep(15)
    processes["afs"][0].kill()

    print("Running system for 30 seconds after primary failure")
    time.sleep(30)

    completed_tasks = []
    for worker in range(num_workers):
        completed_tasks.append(len(check_log(log_dir, "worker" + str(worker) + ".log", "Complete")))
        
    if sum(completed_tasks) > 0:
        print("Success: Workers continued processing")
        for worker in range(num_workers):
            print("Worker " + str(worker) + " completed " + str(completed_tasks[worker]) + " tasks.")
    else:
        print("Failure: workers did not continue processing")

    quit_system(processes)


def afs_replication_test(num_replicas, cache_dir, initial_address, afs_directory, log_dir, max_retries=3, retry_delay=1, primary_server=0):
    replicas_str = build_local_address_strings(num_replicas, initial_address)
    system = {
        "afs": [],
        "client": None
    }
    system["afs"] = run_afs_system(num_replicas, initial_address, afs_directory, log_dir, primary_server=0)
    system["client"] = run_client_subprocess(0, replicas_str, log_dir, max_retries=max_retries, retry_delay=retry_delay)

    print("Sleeping for 3 seconds")
    time.sleep(3)

    test_output_file = "test_cli"+str(0) + ".txt"

    #Search replica directories
    replica_dirs = [os.path.join(afs_directory, str(i)) for i in num_replicas]
    matches = 0
    for replica_dir in replica_dirs:
        matches = matches + len(list(filter(lambda x: x == test_output_file, os.listdir(replica_dir))))
    
    print("Found " + str(matches) + " files matching the test case")
    if matches == 0:
        print("Write failed")
    if matches == 1:
        print("Write success, no duplicates")
    if matches > 1:
        print("Write success, duplicates found")


    #Waiting for 10 seconds
    time.sleep(10)

    
    
    quit_system(system)

#Coordinator shutdown
def coordinator_failure(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=5):
    processes = run_system(num_replicas, num_workers, initial_address, afs_directory, log_dir, snapshots_dir, fermats_number=fermats_number)

    print("Waiting for 45 seconds to have snapshot")
    time.sleep(45)


    print("Killing coordinator and subprocesses")

    #Kill coordinator and any subprocesses
    for pid in processes["coordinator"]:
        psutil.Process(pid).kill()

    print("Starting new coordinator")
    processes["coordinator"] = run_coordinator_subprocess(log_dir)

    #Check snapshots directory for snapshots. 
    print("Snapshots content")
    directory_contents = os.listdir(snapshots_dir)
    if len(directory_contents) > 0:
        print(directory_contents)
    else:
        print("No snapshots created")


#Three tests:
#1 Run workers and get snapshots

#2 Kill a worker and see what happens

#3 Test if AFS primary server fails
def all_tests():
    test_num = 1
    print("Running test 1, snapshot creation")
    worker_snapshots_test(3, 3, "localhost:8080", "data", "logs/test" + str(test_num), "snapshots/")
    print("----------")
    test_num = test_num + 1
    print("Running test 2, worker failure")
    worker_failure_test(3, 3, "localhost:8080", "data", "logs/test" + str(test_num), "snapshots/")
    print("----------")
    test_num = test_num + 1
    print("Running test 3, AFS primary failure")
    afs_primary_failure_test(3, 3, "localhost:8080", "data", "logs/test" + str(test_num), "snapshots/")
    print("----------")
    test_num = test_num + 1
    print("Running test 4, AFS replication test")
    afs_replication_test(2, "tmp/", "localhost:8080", "data", "logs/test")
    print("----------")
    test_num = test_num + 1
    print("Running test 5, coordinator failure")
    coordinator_failure(3, 3, "localhost:8080", "data", "logs/test" + str(test_num), "snapshots/")
    print("----------")


if __name__ == "__main__":
    #all_tests()
    afs_replication_test(2, "tmp/", "localhost:8080", "data", "logs/test")