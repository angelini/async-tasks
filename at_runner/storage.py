import datetime as dt

import psycopg2  # type: ignore
import typing as t


class LazyConnection:
    user: str
    __conn: t.Any

    def __init__(self, user: str) -> None:
        self.user = user
        self.__conn = None

    def __getattr__(self, attr: str) -> t.Any:
        if not self.__conn:
            self.__conn = self.__connect()
        return getattr(self.__conn, attr)

    def __connect(self) -> t.Any:
        return psycopg2.connect(f'user={self.user}')


class Storage:
    conn: LazyConnection

    def __init__(self, user: str) -> None:
        self.conn = LazyConnection(user)

    def setup(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS
                locks (
                    name         varchar(256),
                    id           bigint,
                    created_at   timestamp NOT NULL,
                    completed_at timestamp,
                    PRIMARY KEY (name, id)
                );

            CREATE TABLE IF NOT EXISTS
                invalid (
                    name        varchar(256),
                    id          bigint,
                    created_at  timestamp NOT NULL,
                    data        bytea NOT NULL,
                    errors      text,
                    PRIMARY KEY (name, id)
                );
        ''')
        self.conn.commit()

    def lock(self, name: str, id: int, timeout: dt.timedelta) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO locks (name, id, created_at, completed_at)
                    VALUES (%s, %s, %s, %s);
            ''', (name, id, dt.datetime.now(), None))
            self.conn.commit()
            return True

        except psycopg2.errors.UniqueViolation:
            self.conn.rollback()
            now = dt.datetime.now()
            cursor.execute('''
                UPDATE locks
                    SET created_at = %s
                WHERE name = %s
                  AND id = %s
                  AND created_at < %s
                  AND completed_at IS NULL
            ''', (now, name, id, now - timeout))
            self.conn.commit()
            return bool(cursor.rowcount == 1)

        return False

    def complete(self, name: str, id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE locks
                SET completed_at = %s
            WHERE name = %s
              AND id = %s
              AND completed_at IS NULL;
        ''', (dt.datetime.now(), name, id))
        self.conn.commit()
        return bool(cursor.rowcount == 1)

    def release(self, name: str, id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute('''
            DELETE FROM locks
                WHERE name = %s
                  AND id = %s
                  AND completed_at IS NULL;
        ''', (name, id))
        self.conn.commit()
        return bool(cursor.rowcount == 1)

    def invalid(self, name: str, id: int, data: bytes, errors: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO invalid (name, id, created_at, data, errors)
                    VALUES (%s, %s, %s, %s, %s);
            ''', (name, id, dt.datetime.now(), data, errors))
            self.conn.commit()

        except psycopg2.errors.UniqueViolation:
            return True

        return bool(cursor.rowcount == 1)
