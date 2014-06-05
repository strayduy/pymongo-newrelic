import newrelic.api
import types
from newrelic.api.database_trace import DatabaseTrace
from newrelic.api.object_wrapper import wrap_object
from .decoder import mongodb_decode_wire_protocol
from logging import getLogger

LOG = getLogger(__name__)

DEEP_DECODE = True # This should come out of the newrelic config...


def instrument_pymongo_connection(module):
    wrap_object(module, 'Connection._send_message', PyMongoTraceWrapper)
    wrap_object(module, 'Connection._send_message_with_response', PyMongoTraceWrapper)


class PyMongoTraceWrapper(object):
    def __init__(self, wrapped):
        if type(wrapped) == types.TupleType:
            (instance, wrapped) = wrapped
        else:
            instance = None

        newrelic.api.object_wrapper.update_wrapper(self, wrapped)

        self._nr_instance = instance
        self._nr_next_object = wrapped

        if not hasattr(self, '_nr_last_object'):
            self._nr_last_object = wrapped

    def __get__(self, instance, klass):
        if instance is None:
            return self
        descriptor = self._nr_next_object.__get__(instance, klass)
        return self.__class__((instance, descriptor),)

    def __call__(self, *args, **kwargs):
        transaction = newrelic.api.transaction.current_transaction()
        if not transaction:
            return self._nr_next_object(*args, **kwargs)

        wire = None
        try:
            wire = mongodb_decode_wire_protocol(args[1][1], DEEP_DECODE)
            sql = self.to_sql(wire)
        except Exception, e:
            sql = "UNKNOWN %r %s" % (wire, e)

        with DatabaseTrace(transaction, sql, None):
            return self._nr_next_object(*args, **kwargs)

    def to_sql(self, unpacked):
        op = unpacked.pop('op')
        unpacked.pop('msg_id', None)
        collection = unpacked.pop('collection', None)
        if op == 'query':
            query = unpacked.pop('query')
            return "SELECT data FROM %s WHERE q = %s WITH %s" % (collection,
                                                         anon_params(query),
                                                         anon_params(unpacked))
        if op == 'insert':
            return "INSERT INTO %s VALUES () WITH %s" % (collection, anon_params(unpacked))

        if op == 'update':
            query = unpacked.pop('query')
            update = unpacked.pop('update')
            return "UPDATE %s SET %r WHERE %s WITH %s" % (collection,
                                                                 anon_params(update),
                                                                 anon_params(query),
                                                                 unpacked)

        if op == 'delete':
            query = unpacked.pop('spec')
            return "DELETE FROM %s WHERE %s" % (collection,
                                                anon_params(query))

        return "%s USING %s %r " % (collection, op, unpacked)


def anon_params(d):
    d, _ = _strip_params(d)
    return " AND ".join(["%s=%s" % i for i in d.items()])


def _strip_params(d, i=0):
    if not isinstance(d, dict):
        return d
    for k in d:
        if isinstance(d[k], dict):
            _d, i = _strip_params(d[k], i)
            d[k] = _d
        elif isinstance(d[k], basestring):
            d[k] = "%s_%d" % ('string', i)
            i += 1
        else:
            d[k] = "%s_%d" % (type(d[k]).__name__, i)
            i += 1
    return d, i

