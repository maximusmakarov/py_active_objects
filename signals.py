"""Классы для работы с сигналами и событиями"""
from typing import Optional

from .data_structures.linked_list import DualLinkedListItem, DualLinkedList
from .active_objects import ActiveObject


class Signaler:
    """Источник сигналов"""

    def __init__(self):
        self.queue = DualLinkedList()

    def signalNext(self) -> bool:
        """Сигнализировать следующего слушателя"""
        item = self.queue.remove_first()
        if not item:
            return False
        item.owner.signal()
        return self.queue.first is not None

    def signalAll(self):
        """Сигнализировать всех слушателей"""
        item = self.queue.remove_first()
        while item:
            item.owner.signal()
            item = self.queue.remove_first()

    def close(self):
        """Закрыть сигнализатор"""
        self.signalAll()

    def copyFrom(self, signaler: 'Signaler'):
        """Скопировать слушателей из другого сигнализатора"""
        item = signaler.queue.remove_first()
        while item:
            self.queue.add(item)
            item = signaler.queue.remove_first()

    def check(self, listener: 'Listener') -> bool:
        """Проверить и добавить слушателя"""
        if not listener:
            return False
        if listener.queue.in_list(self.queue):
            return False
        self.queue.add(listener.queue)
        return True

    def wait(self, listener: 'Listener'):
        """Добавить слушателя в очередь ожидания"""
        self.check(listener)

    def isQueued(self, listener: 'Listener') -> bool:
        """Проверить, находится ли слушатель в очереди"""
        return listener.queue.in_list(self.queue)

    def hasListeners(self) -> bool:
        """Есть ли слушатели"""
        return self.queue.first is not None


class Listener:
    """Слушатель сигналов"""

    def __init__(self):
        self.queue = DualLinkedListItem(self)

    def wait(self, signaler: Signaler):
        """Ожидать сигнала"""
        signaler.check(self)

    def signal(self):
        """Обработать сигнал"""
        self.queue.remove()

    def is_signaled(self) -> bool:
        """Проверить, получен ли сигнал"""
        return self.queue.list is None

    def remove(self):
        """Удалить из очереди"""
        self.queue.remove()

    def close(self):
        """Закрыть слушателя"""
        self.queue.remove()

    def check(self, signaler: Signaler) -> bool:
        """Проверить наличие в сигнализаторе"""
        return signaler.check(self)


class AOListener(Listener):
    """Слушатель для активных объектов"""

    def __init__(self, owner: ActiveObject):
        super().__init__()
        self.owner = owner

    def signal(self):
        """Обработать сигнал и уведомить владельца"""
        super().signal()
        self.owner.signal()


class SignalPub:
    """Издатель сигналов (устаревший)"""

    def __init__(self, owner=None):
        self.subscribers = DualLinkedList()
        self.owner = owner

    def signal(self):
        """Сигнализировать подписчиков"""
        item = self.subscribers.first
        while item:
            sub = item.owner
            if not sub.edge or not sub.is_set:
                sub.is_set = True
                sub.owner.signal()
            item = item.next

    def close(self):
        """Закрыть издатель"""
        item = self.subscribers.remove_first()
        while item:
            sub = item.owner
            if not sub.edge or not sub.is_set:
                sub.is_set = True
                sub.owner.signal()
            item = self.subscribers.remove_first()


class SignalSub:
    """Подписчик на сигналы (устаревший)"""

    def __init__(self, owner: ActiveObject, edge: bool = False,
                 is_set=False, pub: SignalPub = None):
        self.owner = owner
        self.pub_link = DualLinkedListItem(self)
        self.is_set = is_set
        self.edge = edge
        if pub:
            self.subscribe(pub)

    def subscribe(self, pub: SignalPub):
        """Подписаться на издателя"""
        pub.subscribers.add(self.pub_link)

    def unsubscribe(self):
        """Отписаться от издателя"""
        self.pub_link.remove()

    def is_subscribed(self) -> bool:
        """Проверить подписку"""
        return self.pub_link.in_list()

    def is_active(self) -> bool:
        """Проверить активность"""
        if self.is_set:
            return True
        if not self.pub_link.in_list():
            return True
        return False

    def reset(self) -> bool:
        """Сбросить состояние"""
        res = self.is_active()
        self.is_set = False
        return res

    def close(self):
        """Закрыть подписчик"""
        self.unsubscribe()


class Flag:
    """Флаг с ожиданием состояния"""

    def __init__(self):
        self._wait_up_queue = DualLinkedList()
        self._wait_down_queue = DualLinkedList()
        self.__is_up = False

    def notify_all(self):
        """Уведомить всех ожидающих"""
        if self.__is_up:
            item = self._wait_up_queue.remove_first()
            while item:
                item.owner.owner.signal()
                item = self._wait_up_queue.remove_first()
        else:
            item = self._wait_down_queue.remove_first()
            while item:
                item.owner.owner.signal()
                item = self._wait_down_queue.remove_first()

    def notify(self) -> bool:
        """Уведомить одного ожидающего"""
        if self.__is_up:
            item = self._wait_up_queue.remove_first()
            if not item:
                return False
            item.owner.owner.signal()
            return self._wait_up_queue.first is not None
        else:
            item = self._wait_down_queue.remove_first()
            if not item:
                return False
            item.owner.owner.signal()
            return self._wait_down_queue.first is not None

    def up(self, notify_all: bool = True):
        """Поднять флаг"""
        if self.__is_up:
            return False
        self.__is_up = True
        if notify_all:
            self.notify_all()
        return True

    def down(self, notify_all: bool = True):
        """Опустить флаг"""
        if not self.__is_up:
            return False
        self.__is_up = False
        if notify_all:
            self.notify_all()
        return True

    @property
    def is_up(self) -> bool:
        """Проверить состояние флага"""
        return self.__is_up


class FlagListener:
    """Слушатель состояния флага"""

    def __init__(self, owner: ActiveObject):
        self._wait_queue = DualLinkedListItem(self)
        self.owner: ActiveObject = owner
        self.flag: Optional[Flag] = None

    def close(self):
        """Закрыть слушатель"""
        self._wait_queue.remove()
        self.owner = None
        self.flag = None

    def is_up(self, flag: Flag) -> bool:
        """Проверить, поднят ли флаг (с ожиданием если нет)"""
        self.flag = flag
        if flag.is_up:
            if self._wait_queue.list is flag._wait_up_queue:
                flag._wait_up_queue.remove(self._wait_queue)
            return True
        else:
            if (self._wait_queue.list is None or
                    self._wait_queue.list is not flag._wait_up_queue):
                flag._wait_up_queue.add(self._wait_queue)
            return False

    def is_down(self, flag: Flag) -> bool:
        """Проверить, опущен ли флаг (с ожиданием если нет)"""
        self.flag = flag
        if not flag.is_up:
            if self._wait_queue.list is flag._wait_down_queue:
                flag._wait_down_queue.remove(self._wait_queue)
            return True
        else:
            if (self._wait_queue.list is None or
                    self._wait_queue.list is not flag._wait_down_queue):
                flag._wait_down_queue.add(self._wait_queue)
            return False