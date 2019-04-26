import base64
import datetime as dt

import marshmallow as m  # type: ignore
import typing as t

import at_runner.task as task
import at_runner.storage as storage


class TestSchema(m.Schema):
    id = m.fields.Str()


class TestTask(task.Task):

    def schema(self) -> m.Schema:
        return TestSchema()

    def run(self, id: int, data: t.Any, attributes: t.Dict[str, str],
            timestamp: dt.datetime) -> None:
        print('hello world')


if __name__ == '__main__':
    event: task.Event = {
        'data': base64.b64encode(b'{"id": "1"}'),
        'attributes': {},
    }
    context: task.Context = {
        'event_id': 1,
        'timestamp': dt.datetime.now(),
    }

    store = storage.Storage('postgres')
    store.setup()

    test = TestTask(store, 'test-task', 10)
    test.lock_and_run(event, context)
