"""A place for general utility classes"""
import copy

class ModifyContantDictException(Exception):
    pass

class ConstantDict(dict):
    """A dict that does not allow modification"""
    def __init__(self, *args, **kwargs):
        super(ConstantDict, self).update(*args, **kwargs)

    def __copy__(self):
        """make a shallow copy of the value of the ConstantDict in dict form. 
           The default implementation would return a ConstantDict which can not be changed 
           and therefore can not be used as modifiable dictionary
        """
        dct = {}
        dct.update(self)
        return dct

    def __deepcopy__(self, memo):
        """make a deep copy of the value of the ConstantDict in dict form. 
           The default implementation would return a ConstantDict which can not be changed
           and therefore can not be used as modifiable dictionary
        """
        dct = {}
        dct.update(self)
        deep = copy.deepcopy(dct, memo)
        return deep

    def __setitem__(self, key, value):
        raise ModifyContantDictException("Cannot __setitem__ on ConstantDict")

    def __delitem__(self, key):
        raise ModifyContantDictException("Cannot __delitem__ on ConstantDict")

    def update(self, *args, **kwargs):
        raise ModifyContantDictException("Cannot update on ConstantDict")

    def clear(self, *args, **kwargs):
        raise ModifyContantDictException("Cannot clear on ConstantDict")

    def setdefault(self, key, value=None):
        raise ModifyContantDictException("Cannot setdefault on ConstantDict")

    def pop(self, key):
        raise ModifyContantDictException("Cannot pop on ConstantDict")

    def popitem(self):
        raise ModifyContantDictException("Cannot popitem on ConstantDict")

