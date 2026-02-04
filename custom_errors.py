class InvalidSquirrelQuery(LookupError):
    ''' For any queries that result in no results '''
    def __init__(self, message, start_time, end_time):
        super().__init__(message)
        self.start_time = start_time
        self.end_time = end_time
    def __str__(self):
        return f"{self.message} for query {self.start_time}-{self.end_time}"
