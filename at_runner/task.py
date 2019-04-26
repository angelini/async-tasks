import abc
import base64
import datetime as dt
import json

import marshmallow as m  # type: ignore
import mypy_extensions
import typing as t

import at_runner.storage as storage


Event = mypy_extensions.TypedDict('Event', {
    'data': bytes,
    'attributes': t.Dict[str, str],
})

Context = mypy_extensions.TypedDict('Context', {
    'event_id': int,
    'timestamp': dt.datetime,
})


class Task(abc.ABC):
    store: storage.Storage
    name: str
    timeout: dt.timedelta

    TIMEOUT_BUFFER: dt.timedelta = dt.timedelta(seconds=10)

    def __init__(self, store: storage.Storage, name: str,
                 timeout: int) -> None:
        self.store = store
        self.name = name
        self.timeout = dt.timedelta(seconds=timeout) + self.TIMEOUT_BUFFER

    def lock_and_run(self, event: Event, context: Context) -> None:
        id = self.__id(context)
        if not self.store.lock(self.name, id, self.timeout):
            return

        try:
            data, errors = self.__parse(event)
            if errors:
                if self.store.invalid(self.name, id, event['data'], json.dumps(errors)):
                    return
                else:
                    raise RuntimeError('cannot store invalid event')

            self.run(id, data, event['attributes'], context['timestamp'])

        finally:
            self.store.release(self.name, id)

    @abc.abstractmethod
    def schema(self) -> m.Schema:
        pass

    @abc.abstractmethod
    def run(self, id: int, data: t.Any, attributes: t.Dict[str, str],
            timestamp: dt.datetime) -> None:
        pass

    @staticmethod
    def __id(context: Context) -> int:
        return context['event_id']

    def __parse(self, event: Event) -> t.Tuple[t.Any, t.Dict[str, t.List[str]]]:
        try:
            data, errors = self.schema().loads(
                base64.b64decode(event['data']).decode('utf-8')
            )
            return data, errors

        except json.JSONDecodeError:
            return (None, {'_': ['invalid json']})
