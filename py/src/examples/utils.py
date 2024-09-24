import time

def timing(f):
    def wrap(*args, **kwargs):
        start_time = time.time()
        result = f(*args, **kwargs)
        end_time = time.time()
        print(f"Time taken by {f.__name__}: {end_time - start_time:.2f} seconds")
        return result
    return wrap