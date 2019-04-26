import datetime as dt

import marshmallow as m  # type: ignore
import typing as t

import at_runner.task as task


class OneSchema(m.Schema):
    id = m.fields.Int()


class OneTask(task.Task):

    def schema(self) -> m.Schema:
        return OneSchema()

    def run(self, id: int, data: t.Any, attributes: t.Dict[str, str],
            timestamp: dt.datetime) -> None:
        print('hello world 1')


TaskClass = OneTask()
