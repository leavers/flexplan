from typing import Dict, Hashable, Iterable, Set

from types_ import DependentItem

hashable_types = (str, int, float, tuple, bytes, bool, complex)


class DependencyChain:
    def __init__(self):
        self._priority_of: Dict[Hashable, int] = dict()
        self._sub_of: Dict[Hashable, Set[Hashable]] = dict()
        self._sup_of: Dict[Hashable, Set[Hashable]] = dict()
        self._len: int = 0

    def add(self, name: Hashable, after: DependentItem = None):
        if name is None:
            raise ValueError('param "name" should not be None')
        if name in self._priority_of:
            raise ValueError(f'item {name} already exists')
        if after is None:
            self._sup_of[name] = set()
        else:
            self._sup_of[name] = {after} if isinstance(after, hashable_types) else set(after)

        if name not in self._sub_of:
            self._sub_of[name] = set()
        for pw in self._sup_of[name]:
            if pw not in self._sub_of:
                self._sub_of[pw] = set()
            self._sub_of[pw].add(name)

        self._priority_of[name] = 0
        self._update_priority_of(name)

    def sub_of(self, item: Hashable, deep: bool = False, opt_dep: bool = False) -> Set[Hashable]:
        if not deep:
            return self._sub_of[item]
        res: Set[Hashable] = set()
        group: Set[Hashable] = set()
        pw_group: Set[Hashable] = set()
        group.add(item)
        while len(group) > 0:
            t = group.pop()
            sub_set = self._sub_of[t]
            for s in sub_set:
                if s not in res:
                    group.add(s)
                    if opt_dep:
                        for pw in self._sup_of[s]:
                            if pw == item:
                                continue
                            res.add(pw)
                            pw_group.add(pw)
                            while len(pw_group) > 0:
                                pt = pw_group.pop()
                                s_sup_set = self._sup_of[pt]
                                for ss in s_sup_set:
                                    if ss not in res and ss != item:
                                        pw_group.add(ss)
                                        res.add(ss)
                        group |= self._sup_of[s]
            res |= sub_set
        return res

    def sup_of(self, item: Hashable, deep: bool = False) -> Set[Hashable]:
        if not deep:
            return self._sup_of[item]
        res: Set[Hashable] = set()
        group: Set[Hashable] = set()
        group.add(item)
        while len(group) > 0:
            t = group.pop()
            sup_set = self._sup_of[t]
            for s in sup_set:
                if s not in res:
                    group.add(s)
            res |= sup_set
        return res

    def related_of(self, item: Hashable, deep: bool = False, opt_dep: bool = False) -> Set[Hashable]:
        return self.sub_of(item, deep, opt_dep) | self.sup_of(item, deep)

    def sub_chain(self, items: DependentItem) -> 'DependencyChain':
        items: Iterable[Hashable] = {items} if isinstance(items, hashable_types) else set(items)
        item_set: Set[Hashable] = set()
        for item in items:
            item_set.add(item)
            item_set |= self.related_of(item, deep=True, opt_dep=True)
        res: DependencyChain = DependencyChain()
        for item in item_set:
            res.add(item, after=self._sup_of[item])
        return res

    def _update_priority_of(self, name):
        if 0 == len(self._sub_of[name]) == len(self._sup_of[name]):
            self._priority_of[name] = -1
            return
        priority = self._priority_of[name]
        for pw in self._sup_of[name]:
            if pw not in self._priority_of or -2 == self._priority_of[pw]:
                self._priority_of[name] = -2
                return
            elif self._priority_of[pw] == -1:
                self._priority_of[pw] = 0
                priority = max(priority, 1)
            else:
                priority = max(priority, self._priority_of[pw] + 1)
        self._priority_of[name] = priority
        self._len = max(self._len, priority + 1)
        for sw in self._sub_of[name]:
            self._update_priority_of(sw)

    def _getitem_core(self, index: int) -> Set[Hashable]:
        res = set()
        for key, value in self._priority_of.items():
            if value == index:
                res.add(key)
        return res

    def __contains__(self, item) -> bool:
        return item in self._priority_of

    def __getitem__(self, index: int) -> Set[Hashable]:
        if index < 0 or index >= self._len:
            raise IndexError('index out of range')
        return self._getitem_core(index)

    def __iter__(self):
        level = 0
        length = self._len
        while level < length:
            yield self.__getitem__(level)
            level += 1

    def __len__(self):
        return self._len

    def __str__(self):
        if self._len > 0:
            res = list(map(lambda x: f'{x[0]}={x[1]}', enumerate(self)))
        else:
            res = list()
        ind = self.independent_items()
        res.append('independent={}'.format(ind if len(ind) > 0 else '{}'))
        ivd = self.invalid_items()
        res.append('invalid={}'.format(ivd if len(ivd) > 0 else '{}'))
        return ','.join(res)

    def size(self) -> int:
        return len(self._priority_of)

    def invalid_items(self) -> Set[Hashable]:
        return self._getitem_core(-2)

    def independent_items(self) -> Set[Hashable]:
        return self._getitem_core(-1)

    def dependent_items(self) -> Set[Hashable]:
        all_items = set(self._priority_of.keys())
        return all_items - self._getitem_core(-2) - self._getitem_core(-1)
