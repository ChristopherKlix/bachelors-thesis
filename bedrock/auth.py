"""
auth.py
=======

"""

# Standard library
import datetime as dt
from typing import Any

# Third-party
import bcrypt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import pandas as pd
import psycopg2
from psycopg2 import sql

from settings import settings


security = HTTPBasic()


class User:
    def __init__(
            self,
            user_id: int,
            username: str,
            password_hash: str,
            created_on: dt.datetime,
            last_login: dt.datetime,
        ):
        self._user_id: int = user_id
        self._username: str = username
        self._password_hash: str | None = password_hash
        self._created_on: dt.datetime = created_on
        self._last_login: dt.datetime = last_login

    @property
    def user_id(self) -> int:
        return self._user_id

    @property
    def username(self) -> str:
        return self._username
    
    @property
    def password_hash(self) -> str:
        return self._password_hash
    
    @property
    def created_on(self) -> dt.datetime:
        return self._created_on
    
    @property
    def last_login(self) -> dt.datetime:
        return self._last_login

    def validate_password(self, password: str) -> bool:
        return bcrypt.checkpw(password.encode(), self._password_hash.encode())
    
    @classmethod
    def hash_password(cls, password: str) -> str:
        return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    
    def __dict__(self) -> dict[str, str]:
        last_login: str | None = None

        if self.last_login is not None:
            last_login = self.last_login.isoformat()

        data: dict[str, str] = {
            'id': f'{self.user_id}',
            'username': self.username,
            'password_hash': self.password_hash,
            'created_on': self.created_on.isoformat(),
            'last_login': last_login
        }

        return data


def get_user_from_db(username: str) -> User | None:
    sql_query: sql.Composable = sql.SQL('''
        SELECT * FROM {users} WHERE username = %s
    ''').format(
        users=sql.Identifier('users')
    )

    conn = _get_db_connection()

    with conn.cursor() as cur:
        cur.execute(sql_query, (username,))

        cols = [desc[0] for desc in cur.description]
        df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

    if df.empty:
        return None

    user_id: str = df.loc[0, 'id']
    password_hash: str = df.loc[0, 'password_hash']
    created_on: dt.datetime = df.loc[0, 'created_on']
    last_login: dt.datetime = df.loc[0, 'last_login']

    user: User = User(
        user_id=user_id,
        username=username,
        password_hash=password_hash,
        created_on=created_on,
        last_login=last_login
    )

    return user


def get_all_users_from_db() -> list[User | None]:
    conn = _get_db_connection()

    sql_query: sql.Composable = sql.SQL('''
        SELECT
            username
        FROM {users}
    ''').format(
        users=sql.Identifier('users')
    )

    with conn.cursor() as cur:
        cur.execute(sql_query)

        cols = [desc[0] for desc in cur.description]
        df: pd.DataFrame = pd.DataFrame(cur.fetchall(), columns=cols)

    return df['username'].apply(get_user_from_db).tolist()


def log_in_user(user: User) -> None:
    conn = _get_db_connection()

    sql_query: sql.Composable = sql.SQL('''
        UPDATE {users}
        SET last_login = now()
        WHERE username = %s
    ''').format(
        users=sql.Identifier('users')
    )

    with conn.cursor() as cur:
        cur.execute(sql_query, (user.username,))

    conn.commit()


def _get_db_connection():
    conn = psycopg2.connect(settings.db.dsn)
    
    return conn


def _auth(
        request: Request,
        credentials: HTTPBasicCredentials = Depends(security)
    ):
    request.state.user = credentials.username

    user: User | None = get_user_from_db(username=credentials.username)

    if user is None:
        raise HTTPException(status_code=401)

    if not user.validate_password(credentials.password):
        raise HTTPException(status_code=401)
    
    log_in_user(user)
    
    return credentials
