import unittest
from pipegram.dependency_chain import DependencyChain


class TestDependencyChain(unittest.TestCase):

    def test_remove(self):
        dc = DependencyChain()
        dc.add('a1')
        dc.add('b1', after={'a1'})
        dc.add('b2', after={'a1'})
        dc.add('c1', after={'b1'})
        dc.add('c2', after={'b2'})
        dc.add('d1', after={'a1', 'c1'})
        dc.remove('a1')
        print(dc)
        print(f'{dc._priority_of=}')
        print(f'{dc._sup_of=}')
        print(f'{dc._sub_of=}')

    def test_ignore(self):
        dc = DependencyChain()
        dc.add('a1')
        dc.add('a2')
        dc.add('b1', after={'a1', 'a2'})
        dc.add('c1', after={'b1', 'a1'})
        dc.add('c2', after={'b1', 'a2'})
        dc.ignore('b1')
        self.assertEqual({'a1', 'a2'}, dc.get_level(0))
        self.assertEqual({'c1', 'c2'}, dc.get_level(-1))

    def test_cycle(self):
        dc = DependencyChain()
        dc.add('a1', after='c1')
        dc.add('b1', after='a1')
        dc.add('c1', after='b1')
        self.assertEqual({'a1', 'b1', 'c1'}, dc.invalid_items())

    def test_part_cycle(self):
        dc = DependencyChain()
        dc.add('a1')
        dc.add('b1', after='a1')
        dc.add('c1', after='b1')
        dc.add('d1', after='c1')
        dc.add('e1', after={'d1', 'g1'})
        dc.add('f1', after='e1')
        dc.add('g1', after='f1')
        self.assertEqual({'e1', 'f1', 'g1'}, dc.invalid_items())
