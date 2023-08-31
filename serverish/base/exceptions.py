
class MessengerReaderStopped(Exception):
    pass

class MessengerRequestNoResponse(Exception):
    pass
class MessengerRequestNoResponders(MessengerRequestNoResponse):
    pass

class MessengerRequestCanceled(MessengerRequestNoResponse):
    pass

class MessengerRequestTimeout(MessengerRequestNoResponse):
    pass

class MessengerRequestNoResultYet(MessengerRequestNoResponse):
    pass