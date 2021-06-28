from multiprocessing import Process

def f(name):
    print('hello', name)

if __name__ == '__main__':
    p = []
    p1 = Process(target=f, args=('bob',))
    p2 = Process(target=f, args=('aaa',))
    p.append(p1)
    p.append(p2)
    for i in p:
        i.start()

    p.join()