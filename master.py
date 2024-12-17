import os
import json
import socket
import threading
from flask import Flask, request, jsonify
from config import CONFIG

app = Flask(__name__)

class Master:
    def __init__(self):
        self.workers = CONFIG['workers']
        self.input_files = []
        self.map_tasks = []
        self.reduce_tasks = []
        self.completed_maps = set()
        self.completed_reduces = set()
        self.intermediate_data = {}
        
    def initialize_job(self):
        # Scan input directory and create map tasks
        for filename in os.listdir(CONFIG['input_dir']):
            if filename.endswith('.txt'):
                self.input_files.append(os.path.join(CONFIG['input_dir'], filename))
        
        # Create map tasks
        for i, filepath in enumerate(self.input_files):
            worker_idx = i % len(self.workers)
            self.map_tasks.append({
                'task_id': f'map_{i}',
                'input_file': filepath,
                'worker': self.workers[worker_idx]
            })
        
        # Create reduce tasks
        for i in range(CONFIG['num_reducers']):
            worker_idx = i % len(self.workers)
            self.reduce_tasks.append({
                'task_id': f'reduce_{i}',
                'worker': self.workers[worker_idx]
            })

master = Master()

@app.route('/request_task', methods=['POST'])
def request_task():
    worker_info = request.json
    
    # Check if all tasks are complete
    if len(master.completed_maps) == len(master.map_tasks) and \
       len(master.completed_reduces) == len(master.reduce_tasks):
        return jsonify({'task_type': 'shutdown'})
    
    # Assign map tasks first
    if len(master.completed_maps) < len(master.map_tasks):
        for task in master.map_tasks:
            if task['task_id'] not in master.completed_maps and \
               task['worker']['host'] == worker_info['host'] and \
               task['worker']['port'] == worker_info['port']:
                return jsonify({
                    'task_type': 'map',
                    'task_data': task
                })
    
    # Then assign reduce tasks
    elif len(master.completed_reduces) < len(master.reduce_tasks):
        for task in master.reduce_tasks:
            if task['task_id'] not in master.completed_reduces and \
               task['worker']['host'] == worker_info['host'] and \
               task['worker']['port'] == worker_info['port']:
                # Make sure we have intermediate data before assigning reduce tasks
                if master.intermediate_data:
                    return jsonify({
                        'task_type': 'reduce',
                        'task_data': task,
                        'intermediate_data': master.intermediate_data
                    })
    
    return jsonify({'task_type': 'wait'})

@app.route('/report_completion', methods=['POST'])
def report_completion():
    result = request.json
    if result['task_type'] == 'map':
        master.completed_maps.add(result['task_id'])
        # Store intermediate data
        master.intermediate_data.update(result['intermediate_data'])
    else:
        master.completed_reduces.add(result['task_id'])
    
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    master.initialize_job()
    app.run(host=CONFIG['master']['host'], 
            port=CONFIG['master']['port']) 