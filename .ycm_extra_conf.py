from os import path


proj_root = path.dirname(path.abspath(__file__))

def Settings( **kwargs ):
    return {
        'interpreter_path': path.join(proj_root, 'venv', 'bin', 'python3.8')
    }
