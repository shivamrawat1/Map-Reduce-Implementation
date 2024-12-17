import os
import shutil
import subprocess
import time
import json
from config import CONFIG

def clean_directories():
    """Clean output directory and recreate it"""
    if os.path.exists(CONFIG['output_dir']):
        shutil.rmtree(CONFIG['output_dir'])
    os.makedirs(CONFIG['output_dir'])
    
    # Also clean and recreate input directory
    if os.path.exists(CONFIG['input_dir']):
        shutil.rmtree(CONFIG['input_dir'])
    os.makedirs(CONFIG['input_dir'])

def generate_sample_data():
    """Generate sample text files for testing word count"""
    sample_texts = [
        "MapReduce is a programming model and an associated implementation",
        "for processing and generating big data sets with a parallel",
        "distributed algorithm on a cluster.",
        "A MapReduce program is composed of a map procedure that performs",
        "filtering and sorting and a reduce method that performs a summary operation."
    ]
    
    for i in range(3):
        with open(f"{CONFIG['input_dir']}/text_{i}.txt", 'w') as f:
            for text in sample_texts:
                f.write(text + '\n')

def consolidate_results():
    """Consolidate results from all reducers into a single file"""
    consolidated = {}
    
    # Read and merge all reducer outputs
    for i in range(CONFIG['num_reducers']):
        with open(f"{CONFIG['output_dir']}/result_reduce_{i}.json") as f:
            reducer_data = json.load(f)
            for word, count in reducer_data.items():
                if word in consolidated:
                    consolidated[word] += count
                else:
                    consolidated[word] = count
    
    # Write consolidated results
    with open(f"{CONFIG['output_dir']}/final_result.json", 'w') as f:
        json.dump(consolidated, f, indent=2)

def launch_local():
    """Launch the MapReduce system locally using different ports"""
    clean_directories()
    generate_sample_data()
    
    # Start master
    master_proc = subprocess.Popen(['python', 'master.py'])
    
    # Start workers
    worker_procs = []
    for worker in CONFIG['workers']:
        proc = subprocess.Popen([
            'python', 'worker.py',
            worker['host'], str(worker['port'])
        ])
        worker_procs.append(proc)
    
    try:
        # Monitor output directory for completion
        while True:
            time.sleep(1)
            # Check if all reduce tasks have completed
            expected_files = {f"result_reduce_{i}.json" 
                            for i in range(CONFIG['num_reducers'])}
            existing_files = set(os.listdir(CONFIG['output_dir']))
            
            if expected_files.issubset(existing_files):
                print("\nMapReduce job completed!")
                # Add consolidation step
                consolidate_results()
                print("\nResults consolidated into final_result.json")
                break
                
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        master_proc.terminate()
        for proc in worker_procs:
            proc.terminate()

def launch_distributed():
    """Launch the MapReduce system across multiple machines"""
    clean_directories()
    generate_sample_data()
    
    # Start master on cs0
    master_cmd = f"ssh cs0 'cd {os.getcwd()} && python master.py'"
    master_proc = subprocess.Popen(master_cmd, shell=True)
    
    # Start workers on different machines
    worker_procs = []
    for worker in CONFIG['workers']:
        cmd = f"ssh {worker['host']} 'cd {os.getcwd()} && python worker.py {worker['host']} {worker['port']}'"
        proc = subprocess.Popen(cmd, shell=True)
        worker_procs.append(proc)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        master_proc.terminate()
        for proc in worker_procs:
            proc.terminate()

def store_results_in_db():
    """Store reducer outputs in a database"""
    import sqlite3
    
    conn = sqlite3.connect('mapreduce_results.db')
    c = conn.cursor()
    
    # Create table
    c.execute('''CREATE TABLE IF NOT EXISTS word_counts
                 (word TEXT PRIMARY KEY, count INTEGER)''')
    
    # Read all reducer outputs and insert/update database
    for i in range(CONFIG['num_reducers']):
        with open(f"{CONFIG['output_dir']}/result_reduce_{i}.json") as f:
            data = json.load(f)
            for word, count in data.items():
                c.execute('''INSERT OR REPLACE INTO word_counts (word, count)
                            VALUES (?, ?)''', (word, count))
    
    conn.commit()
    conn.close()

def run_mapreduce_chain():
    """Run a chain of MapReduce jobs"""
    # First MapReduce job - word count
    run_wordcount_job()
    
    # Second MapReduce job - consolidate results
    CONFIG['input_dir'] = CONFIG['output_dir']  # Use previous output as input
    CONFIG['output_dir'] = 'final_output'
    CONFIG['num_reducers'] = 1  # Use single reducer for final consolidation
    
    run_consolidation_job()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--distributed':
        launch_distributed()
    else:
        launch_local() 