import datetime as dt

import marshmallow as m  # type: ignore
import typing as t

import at_runner.task as task


class TwoSchema(m.Schema):
    id = m.fields.Int()
    value = m.fields.Str()


class TwoTask(task.Task):

    def schema(self) -> m.Schema:
        return TwoSchema()

    def run(self, id: int, data: t.Any, attributes: t.Dict[str, str],
            timestamp: dt.datetime) -> None:
        print('hello world 2')


TaskClass = TwoTask