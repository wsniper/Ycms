from os import path

conf = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }
    },
    'handlers': {
        'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
        },
        'with_request_info_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': path.join(path.dirname(path.dirname(path.abspath(__file__))), 'log',
                                  'ycms.log'),
            'mode': 'a',
            'maxBytes': 4096,
            'backupCount': 999999999
        }
    },
    'loggers': {
        'with_request_info': {
            'level': 'INFO',
            'handlers': ['with_request_info_handler']
        }
    },
    'default': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
}
