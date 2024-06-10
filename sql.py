get_parent_stmt = """
    WITH RECURSIVE
        hierarchy AS (
            SELECT
                id,
                ParentId,
                name,
                Type
            FROM
                employees
            WHERE
                id = {user_id}
            UNION ALL
            SELECT
                t.id,
                t.ParentId,
                t.name,
                t.Type
            FROM
                employees t
                JOIN hierarchy h ON t.id = h.ParentId
        )
    SELECT
        *
    FROM
        hierarchy
    WHERE
        ParentId IS NULL;
"""


get_heierarchy_stmt = """
    WITH RECURSIVE
    hierarchy AS (
        SELECT
            id,
            ParentId,
            name,
            Type
        FROM
            employees
        WHERE
            id = {parent_id}
        UNION ALL
        SELECT
            t.id,
            t.ParentId,
            t.name,
            t.Type
        FROM
            employees t
            JOIN hierarchy h ON t.ParentId = h.id
        )
    SELECT
        name
    FROM
        hierarchy
    where
        Type != 2;
"""
