"""AVL дерево для хранения объектов"""
from typing import Optional, Callable, Any, Generator


class TreeNode:
    """Узел AVL дерева"""

    def __init__(self, owner=None):
        self._parent = None
        self._left = None
        self._right = None
        self.FBalance = 0  # Баланс узла
        if owner is not None:
            self.owner = owner

    def get_successor(self) -> Optional['TreeNode']:
        """Получить следующий узел"""
        result = self._right
        if result is not None:
            while result._left is not None:
                result = result._left
        else:
            result = self
            while (result._parent is not None and
                   result._parent._right is result):
                result = result._parent
            return result._parent
        return result

    def get_precessor(self) -> Optional['TreeNode']:
        """Получить предыдущий узел"""
        result = self._left
        if result is not None:
            while result._right is not None:
                result = result._right
        else:
            result = self
            while (result._parent is not None and
                   result._parent._left is result):
                result = result._parent
            return result._parent
        return result

    def get_tree(self) -> Optional[Any]:
        """Получить корень дерева"""
        if self._parent is None:
            return None
        baseNode = self
        while baseNode._parent is not None:
            baseNode = baseNode._parent
        return baseNode.owner

    def in_tree(self) -> bool:
        """Проверить, находится ли узел в дереве"""
        return self._parent is not None

    def remove(self):
        """Удалить узел из дерева"""
        if self._parent is not None:
            t = self.get_tree()
            if hasattr(t, 'remove'):
                t.remove(self)


class Tree:
    """AVL дерево"""

    def __init__(self, Comp: Callable):
        self.count = 0
        self.__base = TreeNode(self)
        self.__root = None
        self.__comp = Comp

    def __set_root(self, root: Optional[TreeNode]):
        self.__root = root
        self.__base._right = root
        self.__base._left = root
        if self.__root is not None:
            self.__root._parent = self.__base

    def get_leftmost(self) -> Optional[TreeNode]:
        """Получить самый левый (минимальный) узел"""
        result = self.__root
        if result is None:
            return result
        while result._left is not None:
            result = result._left
        return result

    def get_rightmost(self) -> Optional[TreeNode]:
        """Получить самый правый (максимальный) узел"""
        result = self.__root
        if result is None:
            return result
        while result._right is not None:
            result = result._right
        return result

    def __balance_after_delete(self, node: TreeNode):
        """Балансировка после удаления"""
        while node is not None:
            if node.FBalance in (1, -1):
                return
            OldParent = node._parent
            if node.FBalance == 0:
                if OldParent is self.__base:
                    return
                if OldParent._left is node:
                    OldParent.FBalance += 1
                else:
                    OldParent.FBalance -= 1
                node = OldParent
            elif node.FBalance == 2:
                OldRight = node._right
                if OldRight.FBalance >= 0:
                    self.__rotate_left(node)
                    node.FBalance = 1 - OldRight.FBalance
                    OldRight.FBalance -= 1
                    node = OldRight
                else:
                    OldRightLeft = OldRight._left
                    self.__rotate_right(OldRight)
                    self.__rotate_left(node)
                    if OldRightLeft.FBalance <= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = -1
                    if OldRightLeft.FBalance >= 0:
                        OldRight.FBalance = 0
                    else:
                        OldRight.FBalance = 1
                    OldRightLeft.FBalance = 0
                    node = OldRightLeft
            else:
                OldLeft = node._left
                if OldLeft.FBalance <= 0:
                    self.__rotate_right(node)
                    node.FBalance = (-1 - OldLeft.FBalance)
                    OldLeft.FBalance += 1
                    node = OldLeft
                else:
                    OldLeftRight = OldLeft._right
                    self.__rotate_left(OldLeft)
                    self.__rotate_right(node)
                    if OldLeftRight.FBalance >= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = 1
                    if OldLeftRight.FBalance <= 0:
                        OldLeft.FBalance = 0
                    else:
                        OldLeft.FBalance = -1
                    OldLeftRight.FBalance = 0
                    node = OldLeftRight

    def __switch_position_with_successor(self, node: TreeNode,
                                         ASuccessor: TreeNode):
        """Поменять местами узел с его преемником"""
        OldBalance = node.FBalance
        node.FBalance = ASuccessor.FBalance
        ASuccessor.FBalance = OldBalance

        OldParent = node._parent
        OldLeft = node._left
        OldRight = node._right
        OldSuccParent = ASuccessor._parent
        OldSuccLeft = ASuccessor._left
        OldSuccRight = ASuccessor._right

        if OldParent is not self.__base:
            if OldParent._left is node:
                OldParent._left = ASuccessor
            else:
                OldParent._right = ASuccessor
        else:
            self.__set_root(ASuccessor)
        ASuccessor._parent = OldParent

        if OldSuccParent is not node:
            if OldSuccParent._left is ASuccessor:
                OldSuccParent._left = node
            else:
                OldSuccParent._right = node
            ASuccessor._right = OldRight
            node._parent = OldSuccParent
            if OldRight is not None:
                OldRight._parent = ASuccessor
        else:
            ASuccessor._right = node
            node._parent = ASuccessor

        node._left = OldSuccLeft
        if OldSuccLeft is not None:
            OldSuccLeft._parent = node
        node._right = OldSuccRight
        if OldSuccRight is not None:
            OldSuccRight._parent = node
        ASuccessor._left = OldLeft
        if OldLeft is not None:
            OldLeft._parent = ASuccessor

    def __balance_after_insert(self, node: TreeNode):
        """Балансировка после вставки"""
        OldParent = node._parent
        while OldParent is not self.__base:
            if OldParent._left is node:
                OldParent.FBalance -= 1
                if OldParent.FBalance == 0:
                    return
                if OldParent.FBalance == -1:
                    node = OldParent
                    OldParent = node._parent
                    continue
                if node.FBalance == -1:
                    self.__rotate_right(OldParent)
                    node.FBalance = 0
                    OldParent.FBalance = 0
                else:
                    OldRight = node._right
                    self.__rotate_left(node)
                    self.__rotate_right(OldParent)
                    if OldRight.FBalance <= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = -1
                    if OldRight.FBalance == -1:
                        OldParent.FBalance = 1
                    else:
                        OldParent.FBalance = 0
                    OldRight.FBalance = 0
                return
            else:
                OldParent.FBalance += 1
                if OldParent.FBalance == 0:
                    return
                if OldParent.FBalance == 1:
                    node = OldParent
                    OldParent = node._parent
                    continue
                if node.FBalance == 1:
                    self.__rotate_left(OldParent)
                    node.FBalance = 0
                    OldParent.FBalance = 0
                else:
                    OldLeft = node._left
                    self.__rotate_right(node)
                    self.__rotate_left(OldParent)
                    if OldLeft.FBalance >= 0:
                        node.FBalance = 0
                    else:
                        node.FBalance = 1
                    if OldLeft.FBalance == 1:
                        OldParent.FBalance = -1
                    else:
                        OldParent.FBalance = 0
                    OldLeft.FBalance = 0
                return

    def __rotate_left(self, node: TreeNode):
        """Левый поворот"""
        OldRight = node._right
        OldRightLeft = OldRight._left
        AParent = node._parent
        if AParent is not self.__base:
            if AParent._left is node:
                AParent._left = OldRight
            else:
                AParent._right = OldRight
        else:
            self.__set_root(OldRight)
        OldRight._parent = AParent
        node._parent = OldRight
        node._right = OldRightLeft
        if OldRightLeft is not None:
            OldRightLeft._parent = node
        OldRight._left = node

    def __rotate_right(self, node: TreeNode):
        """Правый поворот"""
        OldLeft = node._left
        OldLeftRight = OldLeft._right
        AParent = node._parent
        if AParent is not self.__base:
            if AParent._left is node:
                AParent._left = OldLeft
            else:
                AParent._right = OldLeft
        else:
            self.__set_root(OldLeft)
        OldLeft._parent = AParent
        node._parent = OldLeft
        node._left = OldLeftRight
        if OldLeftRight is not None:
            OldLeftRight._parent = node
        OldLeft._right = node

    def remove(self, node: TreeNode):
        """Удалить узел"""
        if node._parent is None:
            return
        if node._left is not None and node._right is not None:
            self.__switch_position_with_successor(node, node.get_successor())
        OldParent = node._parent
        node._parent = None
        if node._left is not None:
            Child = node._left
        else:
            Child = node._right
        if Child is not None:
            Child._parent = OldParent
        if OldParent is not self.__base:
            if OldParent._left is node:
                OldParent._left = Child
                OldParent.FBalance += 1
            else:
                OldParent._right = Child
                OldParent.FBalance -= 1
            self.__balance_after_delete(OldParent)
        else:
            self.__set_root(Child)
        self.count -= 1

    def add(self, node: TreeNode, Comp: Callable = None):
        """Добавить узел"""
        if Comp is None:
            Comp = self.__comp
        if node._parent is not None:
            node.remove()
        node._left = None
        node._right = None
        node.FBalance = 0
        self.count += 1
        if self.__root is not None:
            InsertPos = self.__find_insert_pos(node, Comp)
            InsertComp = Comp(node, InsertPos)
            node._parent = InsertPos
            if InsertComp < 0:
                InsertPos._left = node
            else:
                InsertPos._right = node
            self.__balance_after_insert(node)
        else:
            self.__set_root(node)

    def __find_insert_pos(self, node: TreeNode,
                          Comp: Callable = None) -> TreeNode:
        """Найти позицию для вставки"""
        if Comp is None:
            Comp = self.__comp
        result = self.__root
        while result is not None:
            c = Comp(node, result)
            if c < 0:
                if result._left is not None:
                    result = result._left
                else:
                    return result
            else:
                if result._right is not None:
                    result = result._right
                else:
                    return result
        return result

    def find_nearest(self, data: Any, Comp: Callable = None) -> Optional[TreeNode]:
        """Найти ближайший узел"""
        if Comp is None:
            Comp = self.__comp
        result = self.__root
        while result is not None:
            c = Comp(data, result)
            if c == 0:
                return result
            if c < 0:
                if result._left is not None:
                    result = result._left
                else:
                    return result
            else:
                if result._right is not None:
                    result = result._right
                else:
                    return result
        return result

    def find(self, Data: Any, Comp: Callable = None) -> Optional[TreeNode]:
        """Найти узел"""
        if Comp is None:
            Comp = self.__comp
        result = self.__root
        while result is not None:
            c = Comp(Data, result)
            if c == 0:
                return result
            if c < 0:
                result = result._left
            else:
                result = result._right
        return result

    def find_or_add(self, node: TreeNode,
                    Comp: Callable = None) -> Optional[TreeNode]:
        """Найти или добавить узел"""
        if Comp is None:
            Comp = self.__comp
        if self.__root is not None:
            InsertPos = self.__root
            while InsertPos is not None:
                insert_comp = Comp(node, InsertPos)
                if insert_comp < 0:
                    if InsertPos._left is not None:
                        InsertPos = InsertPos._left
                    else:
                        break
                else:
                    if insert_comp == 0:
                        return InsertPos
                    if InsertPos._right is not None:
                        InsertPos = InsertPos._right
                    else:
                        break
            insert_comp = Comp(node, InsertPos)
            node.FBalance = 0
            node._left = None
            node._right = None
            node._parent = InsertPos
            if insert_comp < 0:
                InsertPos._left = node
            else:
                InsertPos._right = node
            self.__balance_after_insert(node)
        else:
            node.FBalance = 0
            node._left = None
            node._right = None
            self.__set_root(node)
        self.count += 1
        return None

    def find_leftmost_ge(self, Data: Any,
                         Comp: Callable = None) -> Optional[TreeNode]:
        """Найти самый левый узел >= Data"""
        if Comp is None:
            Comp = self.__comp
        result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n <= 0:
                result = node
                node = node._left
            else:
                node = node._right
        return result

    def find_rightmost_le(self, Data: Any,
                          Comp: Callable = None) -> Optional[TreeNode]:
        """Найти самый правый узел <= Data"""
        if Comp is None:
            Comp = self.__comp
        result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n < 0:
                node = node._left
            else:
                result = node
                node = node._right
        return result

    def find_leftmost_eq(self, Data: Any,
                         Comp: Callable = None) -> Optional[TreeNode]:
        """Найти самый левый узел == Data"""
        if Comp is None:
            Comp = self.__comp
        result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n <= 0:
                if n == 0:
                    result = node
                node = node._left
            else:
                node = node._right
        return result

    def find_rightmost_eq(self, Data: Any,
                          Comp: Callable = None) -> Optional[TreeNode]:
        """Найти самый правый узел == Data"""
        if Comp is None:
            Comp = self.__comp
        result = None
        node = self.__root
        while node is not None:
            n = Comp(Data, node)
            if n < 0:
                node = node._left
            else:
                if n == 0:
                    result = node
                node = node._right
        return result

    def for_each(self, func: Callable):
        """Выполнить функцию для каждого узла"""

        def process_node(node: TreeNode):
            if node._left is not None:
                process_node(node._left)
            if node._right is not None:
                process_node(node._right)
            func(node)

        if self.__root is not None:
            process_node(self.__root)

    def iter(self, backward: bool = False) -> Generator[TreeNode, None, None]:
        """Итератор по узлам"""
        if backward:
            node = self.get_rightmost()
            while node is not None:
                yield node
                node = node.get_precessor()
        else:
            node = self.get_leftmost()
            while node is not None:
                yield node
                node = node.get_successor()