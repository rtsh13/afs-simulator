import time

class Task(object):
    def __init__(self, task_id, filename, priority=0):
        self.task_id = task_id
        self.filename = filename
        self.priority = priority
        self.assignedTo = None
        self.assignedAt = None
        self.completed = False
        self.createdAt = time.time()