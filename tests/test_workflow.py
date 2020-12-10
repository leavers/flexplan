import time
import unittest
from pipegram.workflow2 import Workflow


def a1(*args, **kwargs):
    print('a1 starts', args, kwargs)
    time.sleep(4)
    return 'a1'


def a2(*args, **kwargs):
    print('a2 starts', args, kwargs)
    time.sleep(1)
    return 'a2'


def a3(*args, **kwargs):
    print('a3 starts', args, kwargs)
    time.sleep(1)
    return 'a3'


def a4(*args, **kwargs):
    print('a4 starts', args, kwargs)
    time.sleep(1)
    return 'a4'


def b1(*args, **kwargs):
    print('b1 starts', args, kwargs)
    time.sleep(1)
    return 'b1'


def b2(*args, **kwargs):
    print('b2 starts', args, kwargs)
    time.sleep(1)
    return 'b2'


def b3(*args, **kwargs):
    print('b3 starts', args, kwargs)
    time.sleep(1)
    return 'b3'


def b4(*args, **kwargs):
    print('b4 starts', args, kwargs)
    time.sleep(1)
    return 'b4'


def c1(*args, **kwargs):
    print('c1 starts', args, kwargs)
    time.sleep(1)
    return 'c1'


def c2(*args, **kwargs):
    print('c2 starts', args, kwargs)
    time.sleep(1)
    return 'c2'


def i1(*args, **kwargs):
    print('i1 starts', args, kwargs)
    time.sleep(1)


def i2(*args, **kwargs):
    print('i2 starts', args, kwargs)
    time.sleep(1)


def i3(*args, **kwargs):
    print('i3 starts', args, kwargs)
    time.sleep(1)


class TestWorkflow(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        pass

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_workflow_1(self):
        wf = Workflow()
        wf.add('a1', a1, args=('a1',), kwargs={'value': 'a1'})
        wf.add('a2', a2, args=('a2',), kwargs={'value': 'a2'})
        wf.add('a3', a3, args=('a3',), kwargs={'value': 'a3'})
        wf.add('a4', a4, args=('a4',), kwargs={'value': 'a4'})
        wf.add('b1', b1, args=('b1',), kwargs={'value': wf.p('a1')}, after={'a1'})
        wf.add('b2', b2, args=('b2',), kwargs={'value': 'b2'}, after={'a1', 'a2', 'a3'})
        wf.add('b3', b3, args=('b3',), kwargs={'value': 'b3'}, after={'a2', 'a3'})
        wf.add('b4', b4, args=('b4',), kwargs={'value': 'b4'}, after={'a4'})
        wf.add('c1', c1, args=('c1',), kwargs={'value': 'c1'}, after={'b1'})
        wf.add('c2', c2, args=('c2',), kwargs={'value': 'c2'}, after={'b2', 'b3', 'b4'})
        wf.add('i1', i1, args=('i1',), kwargs={'value': 'i1'})
        wf.add('i2', i2, args=('i2',), kwargs={'value': 'i2'})
        wf.add('i3', i3, args=('i3',), kwargs={'value': 'i3'})
        wf.run()

    def test_workflow_2(self):
        wf = Workflow()
        wf.add('a1', a1, args=('a1',), kwargs={'value': 'a1'})
        wf.add('a2', a2, args=('a2',), kwargs={'value': 'a2'}, after={'a1'})
        wf.add('a3', a3, args=('a3',), kwargs={'value': 'a3'}, after={'a2'})
        wf.add('a4', a4, args=('a4',), kwargs={'value': 'a4'}, after={'a3'})
        wf.add('b1', b1, args=('b1',), kwargs={'value': 'b1'}, after={'a1', 'a2', 'a3', 'a4'})
        wf.add('b2', b2, args=('b2',), kwargs={'value': 'b2'}, after={'a1', 'a2', 'a3', 'a4', 'b1'})
        wf.add('b3', b2, args=('b3',), kwargs={'value': 'b3'}, after={'a1', 'a2', 'a3', 'a4', 'b1', 'b2'})
        wf.add('b4', b4, args=('b4',), kwargs={'value': 'b4'}, after={'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3'})
        wf.add('c1', c1, args=('c1',), kwargs={'value': 'c1'}, after={'a1', 'a2', 'a3', 'a4', 'b1', 'b2', 'b3', 'b4'})
        wf.add('c2', c2, args=('c2',), kwargs={'value': 'c2'}, after={'a1', 'b1'})
        wf.run()
        wf.run()

    def test_workflow_detached_1(self):
        wf = Workflow(single_thread_pool=True)
        wf.add('a1', a1, args=('a1',), kwargs={'value': 'a1'})
        wf.add('a2', a2, args=('a2',), kwargs={'value': 'a2'}, after={'a1'})
        wf.add('a3', a3, args=('a3',), kwargs={'value': 'a3'}, after={'a2'})
        wf.add('a4', a4, args=('a4',), kwargs={'value': 'a4'}, after={'a3'})
        wf.add('b1', b1, args=('b1',), kwargs={'value': 'b1'}, after={'a1', 'a2', 'a3', 'a4'})
        wf.add('b2', b2, args=('b2',), kwargs={'value': 'b2'}, after={'a1', 'a2', 'a3', 'a4', 'b1'})
        wf.add('c1', c1, args=('c1',), kwargs={'value': 'c1'}, after={'a1', 'b1'})
        wf.add('i1', i1, args=('i1',), kwargs={'value': 'i1'})
        wf.add('i2', i2, args=('i2',), kwargs={'value': 'i2'})
        wf.add('i3', i3, args=('i3',), kwargs={'value': 'i3'})
        print('Started')
        result = wf.start()
        print('Join')
        seconds = 0
        while seconds < 15:
            print(f'[1] a1={result.ready("a1")}, a2={result.ready("a2")}, '
                  f'a3={result.ready("a3")}, a4={result.ready("a4")}, '
                  f'b1={result.ready("b1")}, b2={result.ready("b2")}, '
                  f'c1={result.ready("c1")}, '
                  f'i1={result.ready("i1")}, i2={result.ready("i2")}, i3={result.ready("i3")}')
            time.sleep(1)
            seconds += 1
        wf.join()
        print(result.get())
        print('Stated again')
        wf.start()
        print('Join again')
        seconds = 0
        while seconds < 15:
            print(f'[2] a1={result.ready("a1")}, a2={result.ready("a2")}, '
                  f'a3={result.ready("a3")}, a4={result.ready("a4")}, '
                  f'b1={result.ready("b1")}, b2={result.ready("b2")}, '
                  f'c1={result.ready("c1")}')
            time.sleep(1)
            seconds += 1
        wf.join()
        print(result.get())

    def test_workflow_handler_1(self):
        def ticktock(global_dict: dict):
            start_time = global_dict['start_time']
            elapsed = time.time() - start_time
            print(f'elapsed time: {elapsed}')

        wf = Workflow(single_thread_pool=True)
        wf.add('a1', a1, args=('a1',), kwargs={'value': 'a1'})
        wf.add('a2', a2, args=('a2',), kwargs={'value': 'a2'}, after={'a1'})
        wf.add('a3', a3, args=('a3',), kwargs={'value': 'a3'}, after={'a2'})
        wf.add('a4', a4, args=('a4',), kwargs={'value': 'a4'}, after={'a3'})
        wf.add('b1', b1, args=('b1',), kwargs={'value': 'b1'}, after={'a1', 'a2', 'a3', 'a4'})
        wf.add('b2', b2, args=('b2',), kwargs={'value': 'b2'}, after={'a1', 'a2', 'a3', 'a4', 'b1'})
        wf.add('c1', c1, args=('c1',), kwargs={'value': 'c1'}, after={'a1', 'b1'})

        global_dict = {'start_time': time.time()}
        wf.set_heartbeat_handler(ticktock, args=(global_dict,))

        wf.run()

    def test_workflow_handler_2(self):
        wf = Workflow(single_thread_pool=True)
        wf.add('a1', a1, args=('a1',), kwargs={'value': 'a1'})
        wf.add('a2', a2, args=('a2',), kwargs={'value': 'a2'}, after={'a1'})
        wf.add('a3', a3, args=('a3',), kwargs={'value': 'a3'}, after={'a2'})
        wf.add('a4', a4, args=('a4',), kwargs={'value': 'a4'}, after={'a3'})
        wf.add('b1', b1, args=('b1',), kwargs={'value': 'b1'}, after={'a1', 'a2', 'a3', 'a4'})
        wf.add('b2', b2, args=('b2',), kwargs={'value': 'b2'}, after={'a1', 'a2', 'a3', 'a4', 'b1'})
        wf.add('c1', c1, args=('c1',), kwargs={'value': 'c1'}, after={'a1', 'b1'})

        wf2 = Workflow()
        wf2.add('i1', i1, args=('i1',), kwargs={'value': 'i1'})
        wf2.add('i2', i2, args=('i2',), kwargs={'value': 'i2'})
        wf2.add('i3', i3, args=('i3',), kwargs={'value': 'i3'})

        wf.set_heartbeat_handler(wf2)

        wf.run()

    def test_workflow_handler_3(self):
        def ticktock(global_dict: dict):
            start_time = global_dict['start_time']
            elapsed = time.time() - start_time
            print(f'elapsed time: {elapsed}')

        wf = Workflow()
        wf.add('a1', a1, args=('a1',), kwargs={'value': 'a1'})

        global_dict = {'start_time': time.time()}
        wf.set_heartbeat_handler(ticktock, args=(global_dict,))

        wf.run()
