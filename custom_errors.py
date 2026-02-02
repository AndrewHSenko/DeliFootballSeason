class InvalidSquirrelQuery(LookupError):
    ''' For any queries that result in no results '''
    def __init__(self, message, start_time, end_time):
        self.message = message
        super().__init__(self.message)