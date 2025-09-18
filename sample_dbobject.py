import os
import json
import pandas
import datetime
import numpy as np
from functools import wraps
from copy import deepcopy 
from collections import defaultdict
from core.db import DBConnection
from pyspark.sql.functions import when, lit, to_timestamp, col
class SMRObject(object):
    pass

def datacol(**datacol_kwargs):    
    def wrapped_datacol(func):
        dbcol = datacol_kwargs.get('col')
        @wraps(func)
        def inner(*args, **kwargs):
            obj = args[0]            
            if dbcol in obj._datadict.keys():
                return obj._datadict[dbcol]
            return func(*args, **kwargs)
        inner.dbcol = dbcol   
        inner.is_primary = bool(datacol_kwargs.get('primary'))
        return inner
    return wrapped_datacol

def extendedcol(**datacol_kwargs):
    def wrapped_datacol(func):
        dbcol = datacol_kwargs.get('col')
        @wraps(func)
        def inner(*args, **kwargs):
            return func(*args, **kwargs)
        inner.extendedcol = dbcol
        return inner
    return wrapped_datacol

def foreignkey(**datacol_kwargs):
    def wrapped_datacol(func):
        col = datacol_kwargs.get('col')
        tgt_table = datacol_kwargs.get('tgt_table')
        tgt_col = datacol_kwargs.get('tgt_col')
        @wraps(func)
        def inner(*args, **kwargs):
            return func(*args, **kwargs)
        inner.col = col
        inner.tgt_table = tgt_table
        inner.tgt_col = tgt_col
        return inner
    return wrapped_datacol


class DBModelObject(object):
    TABLE_NAME = None
    TABLE_SCHEMA = None

    def __init__(self):
        super(DBModelObject, self).__setattr__('_datadict', {})
        for attr in dir(self):
            func = getattr(self, attr)
            if hasattr(func, 'dbcol'):
                self._datadict[func.dbcol] = getattr(self, attr)()


    def __setattr__(self, key, value):
        from inspect import ismethod
        attr = key
        attr_obj = getattr(self, key)
        if ismethod(attr_obj) and hasattr(attr_obj,'dbcol'):
            self._datadict[attr_obj.dbcol] = value
        else:
            super(DBModelObject, self).__setattr__(attr, value)

    @classmethod
    def get_schema(cls):
        conn = DBConnection()
        return conn.layer_map.get(cls.TABLE_SCHEMA, cls.TABLE_SCHEMA)

    @classmethod
    def get_qualified_table_name(cls):
        schema = cls.get_schema()
        return f"{schema}.{cls.TABLE_NAME}"
    
    @classmethod
    def get_dbcol(cls, attr):
        """
        Returns the database column name for the given attribute.
        """
        func = getattr(cls, attr, None)
        if func and hasattr(func, 'dbcol'):
            return func.dbcol
        raise AttributeError(f"Attribute {attr} does not have a database column associated with it.")
    
    @classmethod
    def filter(cls, **kwargs):
        from objects.resultset import ResultSet
        return ResultSet(cls, **kwargs)
    
    @classmethod
    def join(cls, join_cls, join_type='inner', on=None, alias=None, **kwargs):
        from objects.resultset import ResultSet
        return ResultSet(cls).join(join_cls, join_type=join_type, on=on, alias=alias, **kwargs)

    @classmethod
    def get_base_query(cls, **kwargs):
        cols = [getattr(cls, attr).dbcol for attr in dir(cls) if hasattr(getattr(cls, attr), 'dbcol')]
        # cols = ['%s as %s'%(getattr(cls, attr).dbcol, attr) for attr in dir(cls) if hasattr(getattr(cls, attr), 'dbcol')]
        qry = 'SELECT %s FROM %s WHERE '%(','.join(cols), cls.get_qualified_table_name())
        for attr in kwargs.keys():
            cls_attr = attr.split('__')[0]
            func = getattr(cls, cls_attr)
            if func and hasattr(func, 'dbcol'):
                if isinstance(kwargs[attr], (tuple,list, np.ndarray)):
                    qry = qry + '%s IN (%s) AND '%(func.dbcol, ','.join([f"{v}" if isinstance(v, (int,float)) else f'"{v}"'  for v in kwargs[attr]]))
                else:
                    if str(kwargs[attr]) == 'NULL':
                        qry = qry + '%s is NULL AND '%(func.dbcol)
                    else:
                        qry = qry + '%s AND '%cls.get_predicate_by_field(func.dbcol, attr, kwargs[attr])
        qry = qry[:-4]
        return qry

    @classmethod
    def get_obj2table_mapping(cls):
        """
        Returns a dictionary mapping the object attributes to their corresponding database columns.
        This is useful for serialization and deserialization of the object.
        """
        return dict([(attr, getattr(cls, attr).dbcol) for attr in dir(cls) if hasattr(getattr(cls, attr), 'dbcol')])

    @classmethod
    def get_table2obj_mapping(cls):
        """
        Returns a dictionary mapping the database columns to their corresponding object attributes.
        This is useful for deserializing data from the database into the object.
        """
        return dict([(getattr(cls, attr).dbcol, attr) for attr in dir(cls) if hasattr(getattr(cls, attr), 'dbcol')])

    @classmethod
    def rename_dataframe(cls, df, tbl2obj=True):
        func = cls.get_table2obj_mapping if tbl2obj else cls.get_obj2table_mapping
        if isinstance(df, pandas.DataFrame):
            df.rename(func(), axis=1, inplace=True)
        else:
            for old, new in func().items():
                df = df.withColumnRenamed(old, new)

    @classmethod
    def get_base_detail_list_query(cls, **kwargs):
        cols = ['%s as %s'%(getattr(cls, attr).dbcol, attr) for attr in dir(cls) if hasattr(getattr(cls, attr), 'dbcol')]
        qry = 'SELECT %s FROM %s WHERE '%(','.join(cols), cls.get_qualified_table_name())
        for attr in kwargs.keys():
            cls_attr = attr.split('__')[0]
            func = getattr(cls, cls_attr)
            if func and hasattr(func, 'dbcol'):
                if isinstance(kwargs[attr], (tuple,list)):
                    qry = qry + '%s IN (%s) AND '%(func.dbcol, ','.join([f"{v}" if isinstance(v, (int,float)) else f'"{v}"' for v in kwargs[attr]]))
                else:
                    qry = qry + '%s AND '%cls.get_predicate_by_field(func.dbcol, attr, kwargs[attr])
        qry = qry[:-4]
        return qry

    @classmethod
    def get_predicate_by_field(cls, column_nm, attr, value):        
        value = value if isinstance(value, (int, float)) else '"%s"'%value
        op_map = dict(                
            gt = f'{column_nm} > {value}',
            lt = f'{column_nm} < {value}',
            ne = f'{column_nm} != {value}',
            gte = f'{column_nm} >= {value}',    
            lte = f'{column_nm} <= {value}',            
            isnull = f'{column_nm} IS NULL',
            notnull = f'{column_nm} IS NOT NULL',
            startswith = f'''{column_nm} LIKE "{value.replace('"',"")}%" ''' if isinstance(value, str) else '',
            endswith = f'''{column_nm} LIKE "%{value.replace('"',"")}" ''' if isinstance(value, str) else '',   
            contains = f'''{column_nm} LIKE "%{value.replace('"',"")}%" ''' if isinstance(value, str) else ''
        )
        
        op = ''
        if '__' in attr:
            _, op = attr.split('__')
        return op_map.get(op, f'{column_nm} = {value}')

    @classmethod
    def get_distinct_list_query(cls, cols=None, limit=None, order=None, **kwargs):
        cols = cols or dir(cls)
        s_cols = ['%s as %s'%(getattr(cls, attr).dbcol, attr) for attr in cols if hasattr(getattr(cls, attr), 'dbcol')]
        qry = 'SELECT DISTINCT %s FROM %s WHERE '%(','.join(s_cols), cls.get_qualified_table_name())
        for attr in kwargs.keys():
            cls_attr = attr.split('__')[0] if '__' in attr else attr
            func = getattr(cls, cls_attr)
            if func and hasattr(func, 'dbcol'):
                if isinstance(kwargs[attr], (tuple,list)):
                    qry = qry + '%s IN (%s) AND '%(func.dbcol, ','.join([f"{v}" if isinstance(v, (int,float)) else f'"{v}"' for v in kwargs[attr]]))
                else:
                    qry = qry + '%s AND '%cls.get_predicate_by_field(func.dbcol, attr, kwargs[attr])
        qry = qry[:-4]
        if order:
            qry += ' ORDER BY %s'%(','.join(map(lambda x: x.replace('__', ' '), order)))
            
        if limit:
            qry = qry + ' LIMIT %s'%limit
        return qry

    @classmethod
    def get(cls, extend=False,  **kwargs):
        if extend:
            qry = 'NOT IMPLEMENTED' # cls.get_extended_query(**kwargs)
        else:
            qry = cls.get_base_query(**kwargs)
        with DBConnection() as db:
            df = db.execute(qry, df=True)
            print("Found %s rows" % (len(df)))
            if len(df)!=1:
                raise Exception('Found more than one record for %s'%qry)
            obj_content = df.to_dict(orient='records')[0]
            obj = cls()
            obj._datadict = deepcopy(obj_content)
            return obj

    def to_dict(self):
        ret = {}
        for attr in dir(self):
            func = getattr(self, attr)
            if func and hasattr(func, 'dbcol'):
                ret[func.__name__] = func()
        return ret

    @classmethod
    def get_dataframe(cls, pyspark=True, limit=None, order=None,  **kwargs):
        
        qry = cls.get_base_query(**kwargs)

        if order:
            qry += ' ORDER BY %s'%(','.join(map(lambda x: x.replace('__', ' '), order)))
                        
        if limit and isinstance(limit, int):
            qry = qry + ' LIMIT %s'%limit

        with DBConnection() as db:
            df = db.execute(qry, df=not pyspark)
            if pyspark:
                for old, new in cls.get_table2obj_mapping().items():
                    df = df.withColumnRenamed(old, new)
            else:
                df.rename(cls.get_table2obj_mapping(), axis=1, inplace=True)
            return df

    @classmethod
    def update(cls, set_cols=None, **kwargs):
        #SET: 
        set_qry = ''
        for attr, val in set_cols.items():
            func = getattr(cls, attr)
            if isinstance(val, (int,float)):
                set_qry = set_qry + '%s = %s, '%(func.dbcol, val)
            else:
                set_qry = set_qry + '%s = "%s", '%(func.dbcol, val)

        #WHERE QUERY:
        where = ''
        for attr in kwargs.keys():
            func = getattr(cls, attr)
            if func and hasattr(func, 'dbcol'):
                if isinstance(kwargs[attr], (tuple,list)):
                    where = where + '%s IN (%s) AND '%(func.dbcol, ','.join([f'"{v}"' for v in kwargs[attr]]))
                else:
                    where = where + '%s = "%s" AND '%(func.dbcol, kwargs[attr])

        qry = 'UPDATE %s SET %s WHERE %s '%(cls.get_qualified_table_name(), set_qry[:-2], where[:-4])

        with DBConnection() as db:
            df = db.execute(qry)

    @classmethod
    def insert(cls, **kwargs):
        cols = []
        vals = []
        for attr, val in kwargs.items():
            func = getattr(cls, attr)
            if func and hasattr(func, 'dbcol'):
                cols.append(func.dbcol)
                if isinstance(val, (int,float)):
                    vals.append(str(val))
                else:
                    vals.append(f'"{val}"')
        qry = 'INSERT INTO %s (%s) VALUES (%s)'%(cls.get_qualified_table_name(), ','.join(cols), ','.join(vals))
        with DBConnection() as db:
            db.execute(qry)

    @classmethod
    def insert_dataframe(cls, df):
        df = df if isinstance(df, pandas.DataFrame) else pandas.DataFrame(df)
        with DBConnection() as conn:
            curr_ts = datetime.datetime.now().timestamp()
            df["valid_from"] = curr_ts
            df["valid_to"]= '2200-12-31T05:00'
            cls.rename_dataframe(df, tbl2obj=False)
            if conn.use_databricks_cluster():
                conn.dataframe_insert_spark(df[list(df.columns)], cls.TABLE_NAME, layer=cls.TABLE_SCHEMA)
            else:
                query = conn.dataframe_insert_query(df[list(df.columns)], cls.TABLE_NAME, layer=cls.TABLE_SCHEMA)
                conn.execute(query)

    @classmethod
    def get_max_id(cls):
        pks = [getattr(cls, attr).dbcol for attr in dir(cls) if hasattr(getattr(cls, attr), 'is_primary') and getattr(cls, attr).is_primary]
        query = "SELECT MAX(%s) as max_id FROM %s" %(pks[0], cls.get_qualified_table_name())
        with DBConnection() as db:
            df = db.execute(query, df=True)
            record = df.fillna(0).astype(int).to_dict(orient='records')[0]
            return record["max_id"]


class TemporalDBModelObject(DBModelObject):
    """
    This class is used for objects that have a temporal validity, i.e. they have a valid_from and valid_to columns.
    """
    @datacol(col='VALID_FROM_TS')
    def valid_from(self):
        return ''

    @datacol(col='VALID_TO_TS')
    def valid_to(self):
        return ''
