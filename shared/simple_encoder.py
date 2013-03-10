"""simple object encoder for json serialization
   usage:  simplejson.dumps(obj, cls=ObjectEncoder, indent=4, check_circular=True) """
import simplejson
import datetime
import copy

class SimpleEncoder(simplejson.JSONEncoder):
    def __init__(self, *args, **kwargs):
        simplejson.JSONEncoder.__init__(self, *args, **kwargs)

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, (basestring, int, float, long, list, tuple)):
            return obj
        dct = obj
        if isinstance(obj, object):
            dct = copy.deepcopy(obj.__dict__)
            for k in obj.__dict__:
                if k[0] == "_":
                    del dct[k]
        return dct
