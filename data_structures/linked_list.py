"""Двусвязный список"""
from typing import Optional, Any


class DualLinkedListItem:
    """Элемент двусвязного списка"""

    def __init__(self, owner: Any = None):
        self.next: Optional['DualLinkedListItem'] = None
        self.prev: Optional['DualLinkedListItem'] = None
        self.list: Optional['DualLinkedList'] = None
        if owner is not None:
            self.owner = owner

    def in_list(self, lst: Optional['DualLinkedList'] = None) -> bool:
        """Проверить, находится ли элемент в списке"""
        if lst is None:
            return self.list is not None
        return self.list == lst

    def get_next(self) -> Optional['DualLinkedListItem']:
        """Получить следующий элемент"""
        if self.list is not None:
            return self.next
        return None

    def get_prev(self) -> Optional['DualLinkedListItem']:
        """Получить предыдущий элемент"""
        if self.list is not None:
            return self.prev
        return None

    def remove(self):
        """Удалить элемент из списка"""
        if self.list is not None:
            self.list.remove(self)


class DualLinkedList:
    """Двусвязный список"""

    def __init__(self):
        self.first: Optional[DualLinkedListItem] = None
        self.last: Optional[DualLinkedListItem] = None
        self.count: int = 0

    def add(self, item: DualLinkedListItem):
        """Добавить элемент в конец списка"""
        if item.list is not None:
            item.list.remove(item)
        if self.first is None:
            self.first = item
            self.last = item
            item.next = None
            item.prev = None
        else:
            self.last.next = item
            item.prev = self.last
            item.next = None
            self.last = item
        item.list = self
        self.count += 1

    def add_first(self, item: DualLinkedListItem):
        """Добавить элемент в начало списка"""
        if item.list is not None:
            item.list.remove(item)
        if self.first is None:
            self.first = item
            self.last = item
            item.next = None
            item.prev = None
        else:
            self.first.prev = item
            item.next = self.first
            item.prev = None
            self.first = item
        item.list = self
        self.count += 1

    def clear(self):
        """Очистить список"""
        p = self.first
        while p is not None:
            p2 = p
            p = p.next
            p2.prev = None
            p2.next = None
            p2.list = None
        self.first = None
        self.last = None
        self.count = 0

    def reset(self):
        """Сбросить список"""
        self.count = 0
        self.first = None
        self.last = None

    def remove(self, item: DualLinkedListItem):
        """Удалить элемент из списка"""
        if item.list != self:
            return

        if item.next is None:
            if item.prev is None:
                self.first = None
                self.last = None
            else:
                item.prev.next = None
                self.last = item.prev
        else:
            if item.prev is None:
                self.first = item.next
                self.first.prev = None
            else:
                item.next.prev = item.prev
                item.prev.next = item.next

        self.count -= 1
        item.list = None
        item.prev = None
        item.next = None

    def remove_first(self) -> Optional[DualLinkedListItem]:
        """Удалить первый элемент"""
        result = self.first
        if result is None:
            return None
        if result.next is None:
            self.first = None
            self.last = None
        else:
            self.first = result.next
            self.first.prev = None
        self.count -= 1
        result.list = None
        result.prev = None
        result.next = None
        return result

    def insert_before(self, before: DualLinkedListItem,
                      item: DualLinkedListItem):
        """Вставить элемент перед указанным"""
        if item.list is not None:
            item.list.remove(item)
        if before.prev is None:
            self.add_first(item)
        else:
            before.prev.next = item
            item.prev = before.prev
            item.next = before
            before.prev = item
            item.list = self
            self.count += 1

    def insert_after(self, after: DualLinkedListItem,
                     item: DualLinkedListItem):
        """Вставить элемент после указанного"""
        if item.list is not None:
            item.list.remove(item)
        if after.next is None:
            self.add(item)
        else:
            after.next.prev = item
            item.next = after.next
            item.prev = after
            after.next = item
            item.list = self
            self.count += 1