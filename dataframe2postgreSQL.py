"""
Patched version to support PostgreSQL 
(original version: https://github.com/pydata/pandas/blob/v0.13.1/pandas/io/sql.py)
Adapted functions are:
- added _write_postgresql
- updated table_exist
- updated get_sqltype
- updated get_schema
Collection of query wrappers / abstractions to both facilitate data
retrieval and to reduce dependency on DB-specific API.
"""
from __future__ import print_function
from datetime import datetime, date

from pandas.compat import range, lzip, map, zip
import pandas.compat as compat
import numpy as np
import traceback

from pandas.core.datetools import format as date_format
from pandas.core.api import DataFrame, isnull
import re

#------------------------------------------------------------------------------
# Helper execution function


def execute(sql, con, retry=True, cur=None, params=None):
    """
    Execute the given SQL query using the provided connection object.
    Parameters
    ----------
    sql: string
        Query to be executed
    con: database connection instance
        Database connection.  Must implement PEP249 (Database API v2.0).
    retry: bool
        Not currently implemented
    cur: database cursor, optional
        Must implement PEP249 (Datbase API v2.0).  If cursor is not provided,
        one will be obtained from the database connection.
    params: list or tuple, optional
        List of parameters to pass to execute method.
    Returns
    -------
    Cursor object
    """
    try:
        if cur is None:
            cur = con.cursor()

        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        return cur
    except Exception:
        try:
            con.rollback()
        except Exception:  # pragma: no cover
            pass

        print('Error on sql %s' % sql)
        raise


def _safe_fetch(cur):
    try:
        result = cur.fetchall()
        if not isinstance(result, list):
            result = list(result)
        return result
    except Exception as e:  # pragma: no cover
        excName = e.__class__.__name__
        if excName == 'OperationalError':
            return []


def tquery(sql, con=None, cur=None, retry=True):
    """
    Returns list of tuples corresponding to each row in given sql
    query.
    If only one column selected, then plain list is returned.
    Parameters
    ----------
    sql: string
        SQL query to be executed
    con: SQLConnection or DB API 2.0-compliant connection
    cur: DB API 2.0 cursor
    Provide a specific connection or a specific cursor if you are executing a
    lot of sequential statements and want to commit outside.
    """
    cur = execute(sql, con, cur=cur)
    result = _safe_fetch(cur)

    if con is not None:
        try:
            cur.close()
            con.commit()
        except Exception as e:
            excName = e.__class__.__name__
            if excName == 'OperationalError':  # pragma: no cover
                print('Failed to commit, may need to restart interpreter')
            else:
                raise

            traceback.print_exc()
            if retry:
                return tquery(sql, con=con, retry=False)

    if result and len(result[0]) == 1:
        # python 3 compat
        result = list(lzip(*result)[0])
    elif result is None:  # pragma: no cover
        result = []

    return result


def table_exists(name, con, flavor):
    name = name.lower()
    flavor_map = {
        'postgresql' : "SELECT * FROM pg_catalog.pg_tables where tablename = '%s'" % name}
    query = flavor_map.get(flavor, None)
    return len(tquery(query, con)) > 0


def write_frame(frame, name, con, flavor='postgresql', if_exists='replace', **kwargs):
    """
    Write records stored in a DataFrame to a SQL database.
    Parameters
    ----------
    frame: DataFrame
    name: name of SQL table
    con: an open SQL database connection object
    flavor: {'sqlite', 'mysql', 'oracle'}, default 'sqlite'
    if_exists: {'fail', 'replace', 'append'}, default 'fail'
        fail: If table exists, do nothing.
        replace: If table exists, drop it, recreate it, and insert data.
        append: If table exists, insert data. Create if does not exist.
    """

    if 'append' in kwargs:
        import warnings
        warnings.warn("append is deprecated, use if_exists instead",
                      FutureWarning)
        if kwargs['append']:
            if_exists = 'append'
        else:
            if_exists = 'fail'

    if if_exists not in ('fail', 'replace', 'append'):
        raise ValueError("'%s' is not valid for if_exists" % if_exists)
    
    # kiểm tra tồn tại
    exists = table_exists(name, con, flavor)
    # tạo bảng
    create = None
    if exists:
        if if_exists == 'fail':
            raise ValueError("Table '%s' already exists." % name)
        elif if_exists == 'replace':
            cur = con.cursor()
            cur.execute("DROP TABLE %s;" % name)
            cur.close()
            create = get_schema(frame, name, flavor)
    else:
        create = get_schema(frame, name, flavor)

    # print(create)    
    if create is not None:
        cur = con.cursor()
        cur.execute(create)
        cur.close()

    # tạo query insert
    cur = con.cursor()
    _write_postgresql(frame, name, frame.columns, cur)
    cur.close()
    con.commit()


def _write_postgresql(frame, table, names, cur):

    bracketed_names = ['"' + column + '"' for column in names]
    col_names = ','.join(bracketed_names)

    wildcards = ','.join([r'%s'] * len(names))
    insert_query = 'INSERT INTO %s (%s) VALUES ' % (
        table, col_names )
    data = [tuple(x) for x in frame.values]
    args_str = ','.join(list(map(lambda x: str(x), data)))
    cur.execute(insert_query + args_str) 



def get_sqltype(pytype, flavor):
    sqltype = {'postgresql': 'VARCHAR (100)'}
    # np.floating
    if pytype == np.float:
        sqltype['postgresql'] = 'double precision'
    # np.integer    
    if pytype == np.int:
        sqltype['postgresql'] = 'INTEGER'    
    
    return sqltype[flavor]

# đây
def get_frame_dtypes(fist_row):
    # số thực
    float_re = '[-+]?\d*\.\d*$'
    # số nguyên
    int_re = '^[0-9]+$'

    list_index_float = []
    list_index_int = []
    
    for i, item in enumerate(fist_row):
        if re.search(float_re, item):
            list_index_float.append(i)
        elif re.search(int_re, item):
            list_index_int.append(i) 
    result = ['object' for i in range(195)]
    for i in list_index_float:
        result[i] = np.float
    for i in list_index_int:
        result[i] = np.int
    return result                  


def get_schema(frame, name, flavor, keys=None):
    # cách mình cần làm là gì 
    # Lấy dòng đầu trong data trong (get_data(data))
    # Check các kiểu dữ liệu trong dòng đầu đó => lấy được danh sách datatype (get_data(data))
    # Truyền qua get_schema
    # chỗ thay đổi cấu trúc của bảng

    # Lấy dòng đầu trong data trong (get_data(data))
    first_row = list(frame.iloc[0,:].values)
    # Check các kiểu dữ liệu trong dòng đầu đó => lấy được danh sách datatype (get_data(data))
    frame_types = get_frame_dtypes(first_row)
    # lấy được danh sách datatype get_data(data)
    lookup_type = lambda dtype: get_sqltype(dtype, flavor)

    # Replace spaces in DataFrame column names with _.

    safe_columns = [s.replace(' ', '_').strip() for s in frame.dtypes.index]
    print(frame.dtypes.index)
    column_types = lzip(safe_columns, map(lookup_type, frame_types))

    if flavor == 'postgresql':
        columns = ',\n  '.join('%s %s' % x for x in column_types)

    keystr = ''
    if keys is not None:
        keystr = ', PRIMARY KEY (%s)' % ','.join(keys)
    
    template = """CREATE TABLE %(name)s (
                  %(columns)s
                  %(keystr)s
                  );"""
    create_statement = template % {'name': name, 'columns': columns,
                                   'keystr': keystr}
    print(create_statement)                               
    return create_statement

