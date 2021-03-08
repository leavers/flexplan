from typing import Dict, Iterator, Hashable, Iterable, Set

from pipegram.common import Dependent, hashable_types

_NOT_FOUND = -4
_CYCLE = -3
_INVALID = -2
_INDEPENDENT = -1


class DependencyChain:
    def __init__(self):
        self._priority_of: Dict[Hashable, int] = {}
        self._sub_of: Dict[Hashable, Set[Hashable]] = {}
        self._sup_of: Dict[Hashable, Set[Hashable]] = {}
        self._levels = 0

    def add(self, item: Hashable, after: Dependent = None):
        if item is None:
            raise TypeError('Param "item" should not be None.')
        if self._priority_of.get(item) not in (None, _CYCLE):
            raise ValueError(f'Item "{item}" already exists.')

        sup, sub = self._sup_of, self._sub_of
        if after is None:
            sup[item] = set()
        else:
            after = {after} if isinstance(after, hashable_types) else set(after)
            if item in after:
                raise ValueError(f'Item "{item}" can not depend on itself.')
            sup[item] = after
        if item not in sub:
            sub[item] = set()
        for p in sup[item]:
            if p not in sub:
                sub[p] = set()
            sub[p].add(item)

        self._priority_of[item] = 0
        self._add_priority_of(item)

    def remove(self, item: Hashable):
        if item is None:
            raise TypeError('Param "item" should not be None.')
        self._remove_priority_of(item, item, self._priority_of[item])
        self._remove_and_adjust(item)

    def ignore(self, item: Hashable):
        if item is None:
            raise TypeError('Param "item" should not be None.')
        self._ignore_priority_of(item, self._priority_of[item])
        self._remove_and_adjust(item)

    def sub_of(self, item: Hashable, deep: bool = False, opt_dep: bool = False) -> Set[Hashable]:
        if not deep:
            return self._sub_of[item]
        res: Set[Hashable] = set()
        group: Set[Hashable] = set()
        p_group: Set[Hashable] = set()
        sup, sub = self._sup_of, self._sub_of
        group.add(item)
        while len(group) > 0:
            t = group.pop()
            sub_set = sub[t]
            for b in sub_set:
                if b not in res:
                    group.add(b)
                    if opt_dep:
                        for p in sup[b]:
                            if p == item:
                                continue
                            res.add(p)
                            p_group.add(p)
                            while len(p_group) > 0:
                                pt = p_group.pop()
                                s_sup_set = sup[pt]
                                for sp in s_sup_set:
                                    if sp not in res and sp != item:
                                        p_group.add(sp)
                                        res.add(sp)
                        group |= sup[b]
            res |= sub_set
        return res

    def sup_of(self, item: Hashable, deep: bool = False) -> Set[Hashable]:
        if not deep:
            return self._sup_of[item]
        res: Set[Hashable] = set()
        group: Set[Hashable] = set()
        sup = self._sup_of
        group.add(item)
        while len(group) > 0:
            t = group.pop()
            sup_set = sup[t]
            for s in sup_set:
                if s not in res:
                    group.add(s)
            res |= sup_set
        return res

    def related_of(self, item: Hashable, deep: bool = False, opt_dep: bool = False) -> Set[Hashable]:
        return self.sub_of(item, deep, opt_dep) | self.sup_of(item, deep)

    def sub_chain(self, items: Dependent) -> 'DependencyChain':
        items: Iterable[Hashable] = {items} if isinstance(items, hashable_types) else set(items)
        item_set: Set[Hashable] = set()
        for item in items:
            item_set.add(item)
            item_set |= self.related_of(item, deep=True, opt_dep=True)
        res: DependencyChain = DependencyChain()
        sup = self._sup_of
        for item in item_set:
            res.add(item, after=sup[item])
        return res

    def _remove_and_adjust(self, item: Hashable):
        priority_of, sup, sub = self._priority_of, self._sup_of, self._sub_of
        priority_of.pop(item)
        sup.pop(item)
        sub.pop(item)
        for elem in priority_of.keys():
            sup_set, sub_set = sup[elem], sub[elem]
            sup_set.discard(item)
            sub_set.discard(item)
            if len(sup_set) == 0 and len(sub_set) == 0:
                priority_of[elem] = _INDEPENDENT
        levels = 0
        for priority in priority_of.values():
            if priority >= 0 and priority + 1 > levels:
                levels = priority + 1
        self._levels = levels

    def _ignore_priority_of(self, item: Hashable, priority: int):
        if priority == _INDEPENDENT:
            return
        priority_of, sup, sub = self._priority_of, self._sup_of, self._sub_of
        item_sup_set, item_sub_set = sup[item], sub[item]
        for p in item_sup_set:
            p_sub_set = sub[p]
            for b in item_sub_set:
                sup[b].add(p)
                p_sub_set.add(b)
        self._remove_priority_of(item, item, priority)

    def _remove_priority_of(self, item: Hashable, node: Hashable, priority: int):
        if priority == _INDEPENDENT:
            return
        priority_of, sup, sub = self._priority_of, self._sup_of, self._sub_of
        for b in sub[node]:
            priority_b = 0
            for bp in sup[b]:
                if bp == item:
                    continue
                temp = priority_of[bp]
                if temp == _INVALID:
                    priority_b = _INVALID
                else:
                    priority_b = max(priority_b, temp + 1)
            if priority_b == 0 and len(sub[b]) == 0:
                priority_b = _INDEPENDENT
            priority_of[b] = priority_b
        for b in sub[node]:
            self._remove_priority_of(item, b, priority_of[b])

    def _add_priority_of(self, item: Hashable):
        priority_of, sup, sub = self._priority_of, self._sup_of, self._sub_of
        if 0 == len(sub[item]) == len(sup[item]):
            priority_of[item] = _INDEPENDENT
            return
        priority = priority_of[item]
        for p in sup[item]:
            priority_p = priority_of.get(p)
            if priority_p in (None, _NOT_FOUND):  # not found
                priority_of[p] = _NOT_FOUND
                if priority is None or priority not in (_INVALID, _CYCLE):
                    priority_of[item] = _INVALID
                return
            elif priority_p in (_INVALID, _CYCLE):  # cyclic or normal error
                if priority == _NOT_FOUND:
                    priority_of[item] = _CYCLE
                else:
                    priority_of[item] = _INVALID
                return
            elif priority_p == _INDEPENDENT:  # depends on a item which is independent before
                priority_of[p] = 0
                priority = max(priority, 1)
            else:
                priority = max(priority, priority_p + 1)  # normal dependent item
        priority_of[item] = priority
        self._levels = max(self._levels, priority + 1)
        for b in sub[item]:
            self._add_priority_of(b)

    def _get_level(self, index: int) -> Set[Hashable]:
        res = set()
        for item, priority in self._priority_of.items():
            if priority == index:
                res.add(item)
        return res

    def get_level(self, index: int) -> Set[Hashable]:
        if index >= self._levels or index < -self._levels:
            raise IndexError('Index out of range.')
        if index < 0:
            return self._get_level(self._levels + index)
        return self._get_level(index)

    def levels(self) -> int:
        return self._levels

    def level_items(self) -> Iterator[Set[Hashable]]:
        for i in range(self._levels):
            yield self._get_level(i)

    def __contains__(self, item) -> bool:
        return item in self._priority_of

    def __iter__(self) -> Iterator[Hashable]:
        for item in self._priority_of.keys():
            yield item

    def __len__(self):
        return len(self._priority_of)

    def __str__(self):
        if self._levels > 0:
            res = list(map(lambda x: f'{x[0]}={x[1] if len(x[1]) > 0 else "{}"}',
                           enumerate(self.level_items())))
        else:
            res = list()
        if len((ind := self._get_level(_INDEPENDENT))) > 0:
            res.append(f'independent={ind if len(ind) > 0 else "{}"}')
        if len((ivd := self._get_level(_INVALID))) > 0:
            res.append(f'invalid={ivd if len(ivd) > 0 else "{}"}')
        if len((nfd := self._get_level(_CYCLE))) > 0:
            res.append(f'not found={nfd if len(nfd) > 0 else "{}"}')
        return ','.join(res)

    def size(self) -> int:
        return len(self._priority_of)

    def not_found_items(self) -> Set[Hashable]:
        return self._get_level(_NOT_FOUND)

    def cyclic_items(self) -> Set[Hashable]:
        return self._get_level(_CYCLE)

    def error_dep_items(self) -> Set[Hashable]:
        return self._get_level(_INVALID)

    def invalid_items(self) -> Set[Hashable]:
        return self._get_level(_INVALID) | self._get_level(_CYCLE) | self._get_level(_NOT_FOUND)

    def independent_items(self) -> Set[Hashable]:
        return self._get_level(_INDEPENDENT)

    def dependent_items(self) -> Set[Hashable]:
        all_items = set(self._priority_of.keys())
        return all_items - self.invalid_items() - self.independent_items()
