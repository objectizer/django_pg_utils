from django.db import connections


def get_tables(schema_name='public', connection='default'):
    """Get list of table names based on schema_name. Default schema_name is public."""
    cursor = connections[connection].cursor()
    sql = "SELECT table_name from information_schema.tables WHERE table_schema='%s'"
    sql = sql % schema_name
    cursor.execute(sql)
    rows = cursor.fetchall()
    return ['%s' % r[0] for r in rows]


def get_columns(table, connection='default', schema_name='public'):
    """Get list of column names of a certain table based on schema_name, 
        Default schema_name is public.
    """
    cursor = connections[connection].cursor()
    sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '%s' and table_name = '%s'
    """ % (schema_name, table)
    cursor.execute(sql)
    rows = cursor.fetchall()
    return ['\"%s\"' % r[0] for r in rows]


def get_relations(table, connection='default'):
    """List of relations tied to a certain table.
        Return list of dictionaries of keys:
            primary_key
            from_table
            column_name
            constraint_name
    """
    cursor = connections[connection].cursor()
    sql = """
            SELECT
                t2.oid::regclass::text AS to_table,
                a2.attname AS primary_key,
                t1.relname as from_table,
                a1.attname AS column,
                c.conname AS name,
                c.confupdtype AS on_update,
                c.confdeltype AS on_delete
            FROM
                pg_constraint c
                JOIN pg_class t1 ON c.conrelid = t1.oid
                JOIN pg_class t2 ON c.confrelid = t2.oid
                JOIN pg_attribute a1 ON a1.attnum = c.conkey[1]
                AND a1.attrelid = t1.oid
                JOIN pg_attribute a2 ON a2.attnum = c.confkey[1]
                AND a2.attrelid = t2.oid
                JOIN pg_namespace t3 ON c.connamespace = t3.oid
                WHERE c.contype = 'f'
                AND t2.oid::regclass::text = '%s'
                AND a2.attname = 'id'
            ORDER BY t1.relname, a1.attname
    """ % table
    cursor.execute(sql)
    results = []
    rows = cursor.fetchall()
    for r in rows:
        temp = {}
        temp['primary_key'] = r[1]
        temp['from_table'] = r[2]
        temp['column_name'] = r[3]
        temp['constraint_name'] = r[4]
        results.append(temp)
    return results

def add_constraint(table, constraint_name, column, referenced_table, connection='default', 
        on_update='NO ACTION', on_delete='NO ACTION'):
    """Alter table and add a new constraint.
        on_update default value is 'NO ACTION', possible values ['CASCADE']
        on_delete default value is 'NO ACTION', possible values ['CASCADE']
    """
    cursor = connections[connection].cursor()
    sql = """
    ALTER TABLE %(from_table)s
      ADD CONSTRAINT %(constraint_name)s FOREIGN KEY (%(column_name)s)
            REFERENCES %(parent_table)s (id) MATCH SIMPLE 
            ON UPDATE %(on_update)s ON DELETE %(on_delete)s INITIALLY DEFERRED;
    """ % {'from_table': table,
           'column_name': column,
           'parent_table': referenced_table,
           'constraint_name': constraint_name,
           'on_update': on_update,
           'on_delete': on_delete}
    cursor.execute(sql)


def drop_constraint(table, constraint_name, connection='default'):
    """Alter table and delete constraint.
    """
    cursor = connections[connection].cursor()
    sql = """
    ALTER TABLE %(from_table)s
        DROP CONSTRAINT %(constraint_name)s
        """ % {'from_table': table,
               'constraint_name': constraint_name}
    cursor.execute(sql)
