class MessengerCannotConnect(Exception):
    pass

class MessengerNotConnected(Exception):
    pass

class MessengerReaderStopped(Exception):
    pass

class MessengerRequestNoResponse(Exception):
    pass
class MessengerRequestNoResponders(MessengerRequestNoResponse):
    pass

class MessengerRequestJetStreamSubject(MessengerRequestNoResponse):
    def __init__(self, subject:str) -> None:
        super().__init__(f'Subject {subject} probably declared in JestStream stream. '
                         f'Use pure NATS core subjects for RPC')


class MessengerRequestCanceled(MessengerRequestNoResponse):
    pass

class MessengerRequestTimeout(MessengerRequestNoResponse):
    pass

class MessengerRequestNoResultYet(MessengerRequestNoResponse):
    pass