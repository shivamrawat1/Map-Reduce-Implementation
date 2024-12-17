import re
import json
import time
import requests
from collections import defaultdict
from flask import Flask, request, jsonify
from config import CONFIG

app = Flask(__name__)

class Worker:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
    def map_function(self, input_file):
        """
        Map function that counts words in the input file
        """
        word_counts = defaultdict(int)
        
        with open(input_file, 'r') as f:
            for line in f:
                # Split line into words and count them
                words = line.strip().lower().split()
                for word in words:
                    # Remove punctuation and count if word is not empty
                    word = word.strip('.,!?:;"\'')
                    if word:
                        word_counts[word] += 1
        
        return dict(word_counts)
    
    def reduce_function(self, intermediate_data):
        """
        Reduce function that combines word counts from all mappers
        """
        final_counts = defaultdict(int)
        
        for data in intermediate_data.values():
            for word, count in data.items():
                final_counts[word] += count
        
        return dict(final_counts)
    
    def run(self):
        while True:
            try:
                # Request task from master
                response = requests.post(
                    f"http://{CONFIG['master']['host']}:{CONFIG['master']['port']}/request_task",
                    json={'host': self.host, 'port': self.port}
                )
                
                task = response.json()
                
                if task['task_type'] == 'shutdown':
                    print("Work completed, shutting down...")
                    return  # Exit the run loop
                
                if task['task_type'] == 'wait':
                    time.sleep(1)
                    continue
                
                if task['task_type'] == 'map':
                    # Execute map task
                    result = self.map_function(task['task_data']['input_file'])
                    
                    # Report completion
                    requests.post(
                        f"http://{CONFIG['master']['host']}:{CONFIG['master']['port']}/report_completion",
                        json={
                            'task_type': 'map',
                            'task_id': task['task_data']['task_id'],
                            'intermediate_data': {task['task_data']['task_id']: result}
                        }
                    )
                
                elif task['task_type'] == 'reduce':
                    # Check if we have intermediate data
                    if 'intermediate_data' not in task:
                        print("Waiting for intermediate data...")
                        time.sleep(1)
                        continue
                    
                    # Execute reduce task
                    result = self.reduce_function(task['intermediate_data'])
                    
                    # Save output
                    output_file = f"{CONFIG['output_dir']}/result_{task['task_data']['task_id']}.json"
                    with open(output_file, 'w') as f:
                        json.dump(result, f, indent=2)
                    
                    # Report completion
                    requests.post(
                        f"http://{CONFIG['master']['host']}:{CONFIG['master']['port']}/report_completion",
                        json={
                            'task_type': 'reduce',
                            'task_id': task['task_data']['task_id']
                        }
                    )
            
            except Exception as e:
                print(f"Error in worker: {e}")
                time.sleep(1)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 3:
        print("Usage: python worker.py <host> <port>")
        sys.exit(1)
    
    worker = Worker(sys.argv[1], int(sys.argv[2]))
    worker.run() 