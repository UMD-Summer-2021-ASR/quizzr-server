import time
'''
Simple Python module that contains a collection of testing functions and decorators.
'''

# Source: https://medium.com/pythonhive/python-decorator-to-measure-the-execution-time-of-methods-fa04cb6bb36d
# Modified for Python 3.8.
def timeit(method):
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        if 'log_time' in kwargs:
            name = kwargs.get('log_name', method.__name__.upper())
            kwargs['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed


@timeit
def test_function():
    print("Sleeping...")
    time.sleep(5)
    print("Awake.")


def main():
    # Visit webpages
    pass


if __name__ == '__main__':
    main()
