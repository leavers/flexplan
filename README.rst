Pipegram is a simple workflow written in Python.

Example:

.. code-block:: python

    import time
    from pipegram import Placeholder, Task, Workflow


    def task_a():
        print('[Task A] started, it will cost 2 sec')
        time.sleep(2)
        print('[Task A] finished')
        return 'hello from task A'


    def task_b():
        print('[Task B] started, it will cost 4 sec')
        time.sleep(4)
        print('[Task B] finished')
        return 'hello from task B'


    def task_c(a, b):
        print('[Task C] started when task A and B finished')
        time.sleep(2)
        print(f'[Task C] received return value from task A: {a}')
        print(f'[Task C] received return value from task B: {b}')
        print('[Task C] finished')


    def task_d():
        print('[Task D] started when task B and C finished')
        print('[Task D] finished')


    def test_workflow():
        workflow = Workflow()
        workflow.add(Task(name='Task A',
                          func=task_a))
        workflow.add(Task(name='Task B',
                          func=task_b))
        workflow.add(Task(name='Task C',
                          after=('Task A', 'Task B'),
                          func=task_c,
                          kwargs={'a': Placeholder('Task A'),
                                  'b': Placeholder('Task B')}))
        workflow.add(Task(name='Task D',
                          after=('Task B', 'Task C'),
                          func=task_d))
        workflow.start()


    if __name__ == '__main__':
        test_workflow()