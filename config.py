# Configuration for MapReduce system
CONFIG = {
    'master': {
        'host': 'localhost',
        'port': 5000
    },
    'workers': [
        {'host': 'localhost', 'port': 5001},
        {'host': 'localhost', 'port': 5002},
        {'host': 'localhost', 'port': 5003}
    ],
    'num_mappers': 2,
    'num_reducers': 2,
    'input_dir': 'input',
    'output_dir': 'output',
    'chunk_size': 1024 * 1024  # 1MB chunks
}

# For multi-machine setup, modify workers like this:
MULTI_MACHINE_CONFIG = {
    'master': {
        'host': 'cs0',
        'port': 5000
    },
    'workers': [
        {'host': 'cs0', 'port': 5001},
        {'host': 'cs1', 'port': 5001},
        {'host': 'cs2', 'port': 5001}
    ],
    'num_mappers': 3,
    'num_reducers': 1,
    'input_dir': 'input',
    'output_dir': 'output',
    'chunk_size': 1024 * 1024 
} 