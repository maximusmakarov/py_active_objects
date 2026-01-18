"""Основные классы активных объектов и контроллера"""
from datetime import datetime, timedelta
from typing import Optional, List, Callable, Any, Union
import asyncio
import time

from .data_structures.avl_tree import TreeNode, Tree
from .data_structures.linked_list import DualLinkedListItem, DualLinkedList


class ActiveObjectsController:
    """Контроллер активных объектов"""

    def __init__(self, priority_count: int = 1):
        self.tree_by_t = Tree(_comp_t)
        self.tree_by_id = Tree(_comp_id)
        self.signaled = [DualLinkedList() for _ in range(priority_count)]
        self.terminated: bool = False
        self.emulated_time: Optional[datetime] = None
        self.async_tasks: List[tuple] = []
        self.wakeup_event: Optional[asyncio.Event] = None

    def find(self, type_id, obj_id) -> Optional['ActiveObject']:
        """Найти объект по типу и ID"""
        node = self.tree_by_id.find((type_id, obj_id), _compkey_id)
        return node.owner if node else None

    def now(self) -> datetime:
        """Получить текущее время (реальное или эмулированное)"""
        if self.emulated_time is None:
            return datetime.now()
        return self.emulated_time

    def get_nearest(self) -> Optional['ActiveObject']:
        """Получить ближайший по времени объект"""
        node = self.tree_by_t.get_leftmost()
        return node.owner if node else None

    def wakeup(self):
        """Разбудить цикл обработки"""
        if self.wakeup_event:
            self.wakeup_event.set()

    def process(self, max_count: int = None,
                on_before: Callable = None,
                on_success: Callable = None,
                on_error: Callable = None) -> Optional[datetime]:
        """Обработать очередную порцию объектов"""

        def do(obj: 'ActiveObject'):
            obj.unschedule()
            if on_before and on_before(obj):
                return
            if on_error is None:
                obj._process_internal()
                if on_success:
                    on_success(obj)
            else:
                try:
                    obj._process_internal()
                    if on_success:
                        on_success(obj)
                except Exception as e:
                    on_error(obj, e)

        def remove_next_signaled() -> Optional['ActiveObject']:
            for queue in self.signaled:
                item = queue.remove_first()
                if item:
                    return item.owner
            return None

        while not self.terminated:
            # Обработать асинхронные задачи
            while self.async_tasks:
                try:
                    func, params = self.async_tasks.pop()
                    func(*params)
                except Exception as e:
                    print(f"Async task error: {e}")

            # Обработать запланированные по времени задачи
            obj = self.get_nearest()
            next_time = None
            while obj:
                if obj.get_t() > self.now():
                    next_time = obj.get_t()
                    break
                t = obj.tree_by_t.get_successor()
                next_task = t.owner if t else None
                obj.unschedule()
                obj.signal()
                obj = next_task

            # Обработать сигнализированные задачи
            item = remove_next_signaled()
            if not item:
                return next_time

            n = 10
            while item:
                do(item)
                n -= 1
                if n < 0:
                    break
                if max_count:
                    max_count -= 1
                    if max_count <= 0:
                        return self.now()
                if self.terminated:
                    break
                item = remove_next_signaled()

    def for_each_object(self, type_id, func: Callable):
        """Выполнить функцию для каждого объекта указанного типа"""
        if type_id is None:
            n = self.tree_by_id.get_leftmost()
            while n:
                func(n.owner)
                n = n.get_successor()
        else:
            n = self.tree_by_id.find_leftmost_eq(type_id, _compkey_type)
            while n and n.owner.type_id == type_id:
                n2 = n.get_successor()
                func(n.owner)
                n = n2

    def for_each_object_with_break(self, type_id, func: Callable) -> Any:
        """Выполнить функцию с возможностью прерывания"""
        if type_id is None:
            n = self.tree_by_id.get_leftmost()
            while n:
                n2 = n.get_successor()
                v = func(n.owner)
                if v:
                    return v
                n = n2
        else:
            n = self.tree_by_id.find_leftmost_eq(type_id, _compkey_type)
            while n and n.owner.type_id == type_id:
                n2 = n.get_successor()
                v = func(n.owner)
                if v:
                    return v
                n = n2
        return None

    def get_ids(self, type_id) -> list:
        """Получить список ID объектов указанного типа"""
        res = []
        self.for_each_object(type_id, lambda o: res.append(o.id))
        return res

    def signal(self, type_id=None):
        """Сигнализировать все объекты указанного типа"""
        self.for_each_object(type_id, lambda o: o.signal())

    def terminate(self):
        """Завершить работу контроллера"""
        self.terminated = True
        self.wakeup()

    def threadsafe_async_call(self, func: Callable, params: tuple):
        """Потокобезопасный асинхронный вызов"""
        self.async_tasks.append((func, params))
        self.wakeup()


class ActiveObject:
    """Базовый класс активного объекта"""

    controller: ActiveObjectsController
    type_id = None
    priority: int = 0

    def __init__(self, controller: ActiveObjectsController, obj_id=None):
        self.t: Optional[datetime] = None
        self.id = obj_id
        self.controller = controller
        self.tree_by_t = TreeNode(self)
        self.tree_by_id = TreeNode(self)
        self.signaled = DualLinkedListItem(self)

        if obj_id is not None and self.type_id is not None:
            controller.tree_by_id.add(self.tree_by_id)
        self.signal()

    def _process(self):
        """Основная обработка (переопределяется в подклассах)"""
        pass

    def _process_internal(self):
        """Внутренняя обработка"""
        self._process()

    def is_signaled(self) -> bool:
        """Проверить, сигнализирован ли объект"""
        return self.signaled.in_list()

    def is_scheduled(self) -> bool:
        """Проверить, запланирован ли объект"""
        return self.tree_by_t.in_tree()

    def schedule(self, t: Optional[datetime]):
        """Запланировать выполнение на указанное время"""
        if t is not None:
            if not self.tree_by_t.in_tree() or t < self.t:
                self.controller.tree_by_t.remove(self.tree_by_t)
                self.t = t
                self.controller.tree_by_t.add(self.tree_by_t)

    def schedule_delay(self, delay: timedelta) -> datetime:
        """Запланировать выполнение через указанный интервал"""
        t = self.controller.now() + delay
        self.schedule(t)
        return t

    def schedule_milliseconds(self, delay) -> datetime:
        """Запланировать выполнение через миллисекунды"""
        return self.schedule_delay(timedelta(milliseconds=delay))

    def schedule_seconds(self, delay) -> datetime:
        """Запланировать выполнение через секунды"""
        return self.schedule_delay(timedelta(seconds=delay))

    def schedule_minutes(self, delay) -> datetime:
        """Запланировать выполнение через минуты"""
        return self.schedule_delay(timedelta(minutes=delay))

    def unschedule(self):
        """Отменить запланированное выполнение"""
        self.controller.tree_by_t.remove(self.tree_by_t)
        self.t = None

    def deactivate(self):
        """Деактивировать объект"""
        self.controller.tree_by_t.remove(self.tree_by_t)
        self.t = None
        self.signaled.remove()

    def signal(self):
        """Сигнализировать объект"""
        if not self.signaled.in_list():
            self.controller.signaled[self.priority].add(self.signaled)

    def resignal(self):
        """Пересигнализировать объект (переместить в конец очереди)"""
        self.signaled.remove()
        self.controller.signaled[len(self.controller.signaled) - 1].add(self.signaled)

    def reached(self, t: Optional[datetime]) -> bool:
        """Проверить, достигнуто ли указанное время"""
        if t is None:
            return True
        if t <= self.controller.now():
            return True
        self.schedule(t)
        return False

    def get_t(self) -> Optional[datetime]:
        """Получить время следующего выполнения"""
        return self.t

    def now(self) -> datetime:
        """Получить текущее время от контроллера"""
        return self.controller.now()

    def close(self):
        """Закрыть объект"""
        self.controller.tree_by_t.remove(self.tree_by_t)
        self.controller.tree_by_id.remove(self.tree_by_id)
        self.signaled.remove()


class ActiveObjectWithRetries(ActiveObject):
    """Активный объект с повторными попытками"""

    def __init__(self, controller, obj_id=None):
        super().__init__(controller, obj_id)
        self.__next_retry = None
        self.__next_retry_interval = None
        self.min_retry_interval = 1
        self.max_retry_interval = 60

    def was_error(self) -> bool:
        """Была ли ошибка при последнем выполнении"""
        return self.__next_retry is not None

    def _process_internal(self):
        """Внутренняя обработка с повторами"""
        try:
            if self.__next_retry is None or self.reached(self.__next_retry):
                super()._process_internal()
                self.__next_retry = None
        except Exception:
            if self.__next_retry is None:
                self.__next_retry_interval = self.min_retry_interval
            else:
                self.__next_retry_interval = min(
                    self.__next_retry_interval * 2,
                    self.max_retry_interval
                )
            self.__next_retry = self.schedule_delay(
                timedelta(seconds=self.__next_retry_interval)
            )
            raise


# Функции сравнения для деревьев
def _compkey_id(k, n):
    if k[0] > n.owner.type_id:
        return 1
    elif k[0] < n.owner.type_id:
        return -1
    elif k[1] > n.owner.id:
        return 1
    elif k[1] == n.owner.id:
        return 0
    else:
        return -1


def _compkey_type(k, n):
    if k > n.owner.type_id:
        return 1
    elif k < n.owner.type_id:
        return -1
    else:
        return 0


def _comp_id(n1, n2):
    return _compkey_id((n1.owner.type_id, n1.owner.id), n2)


def _comp_t(n1, n2):
    if n1.owner.t > n2.owner.t:
        return 1
    elif n1.owner.t == n2.owner.t:
        return 0
    else:
        return -1


# Функции циклов выполнения
async def async_loop(controller: ActiveObjectsController):
    """Асинхронный цикл выполнения"""
    controller.terminated = False
    controller.emulated_time = None
    controller.wakeup_event = asyncio.Event()
    controller.wakeup = lambda: controller.wakeup_event.set()

    while not controller.terminated:
        next_time = controller.process()
        if controller.terminated:
            return
        if not controller.wakeup_event.is_set():
            if next_time:
                delta = (next_time - controller.now()).total_seconds()
                if delta > 0:
                    try:
                        await asyncio.wait_for(
                            controller.wakeup_event.wait(),
                            timeout=delta
                        )
                    except asyncio.TimeoutError:
                        pass
            else:
                await controller.wakeup_event.wait()
        controller.wakeup_event.clear()


def simple_loop(controller: ActiveObjectsController):
    """Простой синхронный цикл выполнения"""
    controller.terminated = False
    controller.emulated_time = None
    while not controller.terminated:
        next_time = controller.process()
        if controller.terminated:
            return
        if next_time:
            delta = (next_time - controller.now()).total_seconds()
            if delta > 0:
                time.sleep(delta)


def emulate_asap(controller: ActiveObjectsController, start_time: datetime):
    """Эмуляция выполнения ASAP (как можно скорее)"""
    controller.emulated_time = start_time
    while not controller.terminated:
        controller.emulated_time = controller.process()
        if controller.terminated:
            return
        if controller.emulated_time is None:
            raise Exception('controller.emulated_time is None!')