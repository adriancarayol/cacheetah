from contextlib import contextmanager

@contextmanager
def pipeline(client):
    pipe = client.pipeline()
    yield pipe
    pipe.execute()
