from typing import Tuple
from json import JSONDecoder
from psycopg2 import connect
from psycopg2.extensions import cursor

from sql import get_parent_stmt, get_heierarchy_stmt


def yield_cur():
    connection = connect(host="localhost", database="tenzor", user="", password="")
    cur: cursor = connection.cursor()
    yield cur
    connection.commit()
    cur.close()
    connection.close()


def load_json() -> None:
    """
    Load JSON data from a file  into a PostgreSQL database table.
    """
    for cur in yield_cur():
        with open("json.json", "r") as file:
            decoder: JSONDecoder = JSONDecoder()

            chunk: str
            for chunk in iter(lambda: file.read(4096), b""):
                if not chunk:
                    break
                for json_data in decoder.decode(chunk):
                    values: Tuple[str] = tuple(json_data.values())
                    cur.execute(
                        f"INSERT INTO employees (id, ParentId, Name, Type) VALUES (%s, %s, %s, %s)",
                        values,
                    )


def get_hierarchy(user_id: int) -> str:
    """
    Retrieves the hierarchy of a root user record in the database.

    Args:
        user_id (int): The ID of the user.

    Returns:
        str: The hierarchy of the user in the format "parent: child1, child2, ...". If the user is not found, returns "Record not found".
    """
    for cur in yield_cur():
        cur.execute(get_parent_stmt.format(user_id=user_id))
        parent = cur.fetchone()

        if not parent:
            return "Record not found"

        cur.execute(get_heierarchy_stmt.format(parent_id=parent[0]))
        hierarchy = cur.fetchall()

        if len(hierarchy) <= 1:
            return f"{hierarchy[0][0]}:"

        return f"{hierarchy[0][0]}: {', '.join([child[0] for child in hierarchy[1:]])}"


if __name__ == "__main__":
    load_json()
    print(get_hierarchy(input("User ID: ")))


# мне не нравятся два по сути одинаковых запроса для получения иерархии
# по идее можно использовать ltree
