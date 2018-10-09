
from memory_profiler import profile


@profile
def my_func():
    a = [i ** 2 for i in range(10**5)]
#     a = (i ** 2 for i in range(10**5))
    
    return sum(a)
#     a = [1] * (10 ** 6)
#     b = [2] * (2 * 10 ** 7)
#     del b
#     return a

if __name__ == '__main__':
    my_func()