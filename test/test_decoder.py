import unittest
from pymongo_relic.decoder import mongodb_decode_wire_protocol
import base64
import logging

decode = lambda x: mongodb_decode_wire_protocol(base64.b64decode(x))


class Test(unittest.TestCase):
    def assertQuery(self, byts, result):
        res = decode(base64.b64decode(byts))
        assert res['query'] == result, res

    def test_query(self):
        # admin.$cmd SON([('ismaster', 1)]) -1
        bytes1 = 'OgAAAMCidScAAAAA1AcAAAAAAABhZG1pbi4kY21kAAAAAAD/////EwAAABBpc21hc3RlcgABAAAAAA=='
        res = decode(bytes1)
        assert res['collection'] == 'admin.$cmd', res
        assert res['query'] == {u'ismaster': 1}, res

    def test_update(self):
        # f.db.backfill_jobs.update({'meow': 1}, {'foo': {'$set': 1}})
        updateBytes = 'XwAAAMPdDJQAAAAA0QcAAAAAAABsZmRqX3YyLWZ5cmVob3NlLmJhY2tmaWxsX2pvYnMAAAAAAA8AAAAQbWVvdwABAAAAABkAAAADZm9vAA8AAAAQJHNldAABAAAAAAA='
        res = decode(updateBytes)
        assert res['collection'] == 'lfdj_v2-fyrehose.backfill_jobs', res
        assert res['query'] == {'meow': 1}, res
        assert res['update'] == {'foo': {'$set': 1}}, res

    def test_insert(self):
        # f.db.backfill_jobs.insert({'meow':1}, {'moo':2})
        insertBytes = 'cgAAABcANN4AAAAA0gcAAAAAAABsZmRqX3YyLWZ5cmVob3NlLmJhY2tmaWxsX2pvYnMAIAAAAAdfaWQAUZEoUadNbyhaAAAEEG1lb3cAAQAAAAAfAAAAB19pZABRkShRp01vKFoAAAUQbW9vAAIAAAAA'
        res = decode(insertBytes)
        assert res['collection'] == 'lfdj_v2-fyrehose.backfill_jobs', res
        assert len(res['docs']) == 2, res
        map(lambda x: x.pop('_id', res['docs']), res['docs'])
        assert res['docs'] == [{'meow':1}, {'moo':2}], res

    def test_insert_2(self):
        insertBytes = "IwEAAPCCUXgAAAAA0gcAAAAAAABsZmRqX3YyLWF1ZGl0LmF1ZGl0X2J1c19tZXNzYWdlcwDuAAAAAl9pZAAhAAAANDYzNTY2MmFkOTNlNDhhOTlkZmY3ODE0ZmM5ZTVjM2UAA21zZwCXAAAAAl9wYjJfdHlwZQA4AAAAbGZjb3JlLnYyLmlkZW50aXR5Lm1lc3NhZ2VzX3BiMi5CYWNrQ29tcGF0QXV0aG9yVXBkYXRlZAACYXV0aG9ySWQAEwAAAF91Njk2QGxpdmVmeXJlLmNvbQACX2lkACEAAAA0NjM1NjYyYWQ5M2U0OGE5OWRmZjc4MTRmYzllNWMzZQAAAWNyZWF0ZWRBdADzk6PXRGXUQRBfX2Vycm9yc19fAAAAAAAAQgAAAPCCUXgAAAAA1AcAAAAAAABhZG1pbi4kY21kAAAAAAD/////GwAAABJnZXRsYXN0ZXJyb3IAAQAAAAAAAAAA"
        res = decode(insertBytes)
        assert res == {'docs': [{u'msg': 
                                 {u'_pb2_type': u'lfcore.v2.identity.messages_pb2.BackCompatAuthorUpdated', u'authorId': u'_u696@livefyre.com', u'_id': u'4635662ad93e48a99dff7814fc9e5c3e'}, 
                                 u'_id': u'4635662ad93e48a99dff7814fc9e5c3e', u'createdAt': 1368724318.555905, u'__errors__': 0}],
                       'msg_id': 2018607856, 'continue_on_error': 0, 'collection': 'lfdj_v2-audit.audit_bus_messages', 'op': 'insert'}

if __name__ == "__main__":
    logging.basicConfig()
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()