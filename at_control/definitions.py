import typing as t


class TaskDef:
    name: str
    topic: str
    timeout: int

    def __init__(self, name: str, topic: str, timeout: int) -> None:
        self.name = name
        self.topic = topic
        self.timeout = timeout


class ProjectDef:
    bucket: str
    tasks: t.Dict[str, TaskDef]

    def __init__(self, bucket: str, tasks: t.Dict[str, TaskDef]) -> None:
        self.bucket = bucket
        self.tasks = tasks
