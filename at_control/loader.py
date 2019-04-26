import pathlib as p

import yaml

import at_control.definitions as defs


def load_project(dir: p.Path) -> defs.ProjectDef:
    yaml_path: p.Path = dir.joinpath('project.yaml')
    if not yaml_path.exists():
        raise RuntimeError(f'missing project.yaml in {dir}')

    data = yaml.load(yaml_path.read_text(), Loader=yaml.Loader)

    tasks = dict((task_dir.name, __load_task(dir.name, task_dir))
                 for task_dir in dir.glob('*')
                 if task_dir.is_dir() and
                 not (task_dir.name.startswith('_') or
                      task_dir.name.startswith('.')))

    return defs.ProjectDef(data['bucket'], tasks)


def __load_task(module: str, dir: p.Path) -> defs.TaskDef:
    yaml_path: p.Path = dir.joinpath('task.yaml')
    if not yaml_path.exists():
        raise RuntimeError(f'missing task.yaml in {dir}')

    data = yaml.load(yaml_path.read_text(), Loader=yaml.Loader)
    return defs.TaskDef(dir.name, data['topic'], data['timeout'])
