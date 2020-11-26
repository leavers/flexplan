import weakref


def del_A(name):
    print('An A deleted:' + name)


class A:
    def __init__(self, name):
        print('A created')
        self.name = name
        self._wr = weakref.ref(self, lambda wr, n=self.name: del_A(n))


class B:
    def __init__(self):
        print('B created')


if __name__ == '__main__':
    a = A('a1')
    b = B()
    print('111')
    a = b
    print('222')
    b = a
    print('333')
