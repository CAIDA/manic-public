class FileEmptyError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class InvalidLineFormatError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value    
