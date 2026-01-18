"""Асинхронные задачи"""
import asyncio
from typing import Optional, Callable, List

from .active_objects import ActiveObject, ActiveObjectsController
from .signals import Signaler, Listener


class AbstractTask(ActiveObject):
    """Абстрактная задача"""

    def __init__(self, controller, obj_id=None):
        super().__init__(controller, obj_id)
        self.exit_code: Optional[int] = None
        self._cancel_requested: bool = False
        self._kill_requested: bool = False
        self.error: Optional[Exception] = None
        self.completed_signal = Signaler()
        self.signal()

    def is_completed(self, listener: Optional[Listener] = None) -> bool:
        """Проверить завершение задачи"""
        if self.exit_code is not None:
            return True
        if listener is not None:
            listener.wait(self.completed_signal)
        return False

    def is_cancelled(self) -> bool:
        """Проверить отмену задачи"""
        return self._cancel_requested

    def get_exit_code(self) -> Optional[int]:
        """Получить код завершения"""
        return self.exit_code

    def _process(self):
        """Обработка задачи"""
        if self.is_completed():
            self.completed_signal.signalAll()
            self.close()

    def set_exit_code(self, exit_code: int):
        """Установить код завершения"""
        if self.exit_code is None:
            self.signal()
            self.exit_code = exit_code

    def cancel(self, kill: bool = False):
        """Отменить задачу"""
        if not self._cancel_requested:
            self._cancel_requested = True
            self.signal()
        if kill and not self._kill_requested:
            self._kill_requested = True
            self.signal()

    def close(self):
        """Закрыть задачу"""
        self.completed_signal.close()
        super().close()


class AsyncTaskProcess(AbstractTask):
    """Асинхронный процесс"""

    def __init__(self, controller: ActiveObjectsController,
                 task_func: Optional[Callable] = None):
        self.task_func = task_func
        self.task: Optional[asyncio.Task] = None
        self._cancel_async_task = True
        super().__init__(controller)
        self.task = asyncio.create_task(self._do_task())

    async def _do_task(self):
        """Выполнить задачу"""
        try:
            exit_code = await self.do_task()
            if self._cancel_async_task:
                self._cancel_requested = False
            self.set_exit_code(exit_code if exit_code is not None else -1)
        except Exception as e:
            self.error = e
            self.set_exit_code(-1)
        self.completed_signal.signalAll()
        self.controller.wakeup()

    async def do_task(self) -> int:
        """Основная логика задачи (переопределяется)"""
        if self.task_func:
            return await self.task_func()
        return -1

    def cancel_async_task(self, kill: bool):
        """Отменить асинхронную задачу"""
        if self.task and not self.task.done():
            self.task.cancel("Killed" if kill else "Canceled")
            self.set_exit_code(-1)
            self.completed_signal.signalAll()

    def cancel(self, kill: bool = False):
        """Отменить процесс"""
        super().cancel(kill)
        if self._cancel_async_task:
            self.cancel_async_task(kill)

    def close(self):
        """Закрыть процесс"""
        if self.task:
            self.task.cancel()
            self.task = None
        super().close()


def test_process(task) -> AsyncTaskProcess:
    """Тестовый процесс"""

    async def test():
        await asyncio.sleep(1)
        return 0

    return AsyncTaskProcess(task.controller, test)


class SystemTaskProcess(AsyncTaskProcess):
    """Системный процесс"""

    def __init__(self, controller: ActiveObjectsController,
                 commands: List[str], cwd: Optional[str] = None):
        super().__init__(controller)
        self.commands = commands
        self.cwd = cwd
        self.proc: Optional[asyncio.subprocess.Process] = None
        self._cancel_async_task = False

    async def do_task(self) -> int:
        """Выполнить системную команду"""
        self.proc = await asyncio.create_subprocess_exec(
            self.commands[0],
            *self.commands[1:],
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            cwd=self.cwd
        )
        return await self.proc.wait()

    def cancel(self, kill: bool = False):
        """Отменить системный процесс"""
        super().cancel(kill)
        if self.proc is not None:
            if kill:
                self.proc.kill()
            else:
                self.proc.terminate()
        else:
            super().cancel_async_task(kill)

    def close(self):
        """Закрыть системный процесс"""
        self.proc = None
        super().close()