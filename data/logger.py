import datetime

class Logger:
    def __init__(self, filename):
        self.filename = filename

    def log(self, message):
        timestamped_message = f'{datetime.datetime.now()}: {message}'
        
        with open(self.filename, 'a') as f:
            f.write(timestamped_message + '\n')
        
        print(timestamped_message)