from connopt import get_c
from dataclasses import dataclass


class Field:
    def __init__(self, f_type, required=True, default=None):
        self.f_type = f_type
        self.required = required
        self.default = default

    def validate(self, value):
        if value is None and not self.required:
            return None
        # todo exceptions
        return self.f_type(value)

class Int(Field):
    def __init__(self, required=True, default=None):
        super().__init__(int, required, default)

    def __repr__(self):
        return 'INT'

class IntPK(Field):
    def __init__(self, required=True, default=None):
        super().__init__(int, required, default)

    def __repr__(self):
        return 'INT NOT NULL Primary Key Auto_increment'

class Text(Field):
    def __init__(self, required=True, default=None):
        super().__init__(str, required, default)

    def __repr__(self):
        return 'TEXT'

class Char(Field):
    def __init__(self, required=True, default=None):
        super().__init__(str, required, default)

    def __repr__(self):
        return 'CHAR(255)'

class Initiation:
    def __init__(self):
        self.conn = get_c()
        self.cursor = self.conn.cursor()


class User:
    id = IntPK()
    name = Char()

class Userro:
    id = IntPK()
    name = Text()
#CREATE TABLE Users
#(
#	id INT NOT NULL Primary Key Auto_increment, 
#	user CHAR(100), 
#	hash CHAR(255),
#	UNIQUE (user)
#)Engine=InnoDB;

def create(cls):
    sql='CREATE TABLE '+cls.__qualname__+' ('
    lst=list()
    for i in cls.__dict__:
        lst.append(i)
    for i in lst[1:-3]:
        sql +=i
        sql = sql+' '+str(getattr(cls, i).__class__())+","
    sql = sql[0:-1]
    sql = sql+' ) Engine=InnoDB;'
    init = Initiation()
    init.cursor.execute(sql)

def drop(cls): #в метод можно передать как класс, так и просто строку с названием таблицы
    if not isinstance(cls, str): 
        sql = 'DROP TABLE '+cls.__qualname__+' ;'
    else:
        sql = 'DROP TABLE '+cls+' ;'
    init = Initiation()
    init.cursor.execute(sql)


def checker(dict, fields, name):
    for i in dict:
        if i in fields.keys():
            print('Есть такое')
            if fields[i] == 'char(255)':
                if isinstance(dict[i], str):
                    pass
                else:
                    raise TypeError('Field ('+i+') must be str, not '+str(type(dict[i]))) 
            if fields[i] == 'int(11)':
                if isinstance(dict[i], int):
                    pass
                else:
                    raise TypeError('Field ('+i+') must be int, not '+str(type(dict[i]))) 
            if fields[i] == 'text':
                if isinstance(dict[i], str):
                    pass
                else:
                    raise TypeError('Field ('+i+') must be str, not '+str(type(dict[i]))) 
        else:
            raise ValueError('No such field ('+i+') in table '+name) 

def wh(sql, where, sign):
    key = (*where.keys(),)
    value = (*where.values(),)
    sql = sql + ' WHERE '+str(key)[2:-3]+sign+str(value)[1:-2]+';'
    return sql


def use_table(name):
    class Nclass(Initiation):
        sql = 'Show columns from '+name+';'
        init = Initiation()
        init.cursor.execute(sql)
        __slots__ = tuple()
        fields = dict()
        for i in init.cursor.fetchall():
            __slots__ += (i['Field'],)
            fields.update({i['Field']:i['Type']})

        del sql, i

        @staticmethod
        def insert(dict, init=init, fields=fields):
            checker(dict, fields, name)
            table1 = list()
            table2 = list()
            for i in dict:
                table1.append(i)
                table2.append(dict[i])
            pr = 'INSERT INTO '+name+'('
            for i in table1:
                pr += i+', '
            pr = pr[0:-2]+') values('
            for i in table2:
                pr += '"'+str(i)+'", '
            pr = pr[0:-2]+');'          
            init.cursor.execute(pr)
            return init.cursor.fetchall()

        @staticmethod
        def pure_update(set, where=None, where_more=None, where_less=None, init=init):
            """ Принимает словари
                set = {field: value} SET field = value
                    where: {field: value} WHERE field = value
                    where_more: {field: value} WHERE field >= value
                    where_less: {field: value} WHERE field <= value
            """
            key = (*set.keys(),)
            print(key)
            value = (*set.values(),)
            sql = 'UPDATE '+name+' SET '+str(key)[2:-3]+' = '+str(value)[1:-2]
            if where_more:
                sql = wh(sql, where_more, '>=')
                return init.cursor.execute(sql)
            if where:
                sql = wh(sql, where, '=')
                return init.cursor.execute(sql)
            if where_less:
                sql = wh(sql, where_less, '<=')
                return init.cursor.execute(sql)
            
        @staticmethod
        def remove(where=None, where_more=None, where_less=None, init=init):
            sql = 'DELETE FROM '+name
            if where_more:
                sql = wh(sql, where_more, '>=')
                return init.cursor.execute(sql)
            if where:
                sql = wh(sql, where, '=')
                return init.cursor.execute(sql)
            if where_less:
                sql = wh(sql, where_less, '<=')
                return init.cursor.execute(sql)

        @staticmethod
        def select(field, where=None, where_more=None, where_less=None, limit=None, init=init):
            sql = 'SELECT '+field+' FROM '+name
            if where_more:
                sql = wh(sql, where_more, '>=')
            if where:
                sql = wh(sql, where, '=')
            if where_less:
                sql = wh(sql, where_less, '<=')
            if limit:
                sql = sql[0:-1] + ' LIMIT '+limit+';'
            init.cursor.execute()
            return(init.cursor.fetchall())

            
        @staticmethod
        def commit(conn = init.conn):
            conn.commit()
            print('commited')

        @staticmethod
        def close(conn = init.conn):
            conn.close()
            print('connnection closed')

    return Nclass

#create(Userro)
#n = use_table('Userro')
#n.pure_update(set={'name':'coolin'}, where_more={'id':13})
#n.commit()
#s = Shit()
#wotlk = Book("book", 4, 'wotlk', 'serg')
#wotlk.insert()


#n = use_table('User')
#print(n)
#n.insert({'name':'cool'})

#n.insert({'name':'Sergo8'})
#n.commit()
#for i in n.__dict__:
#    print( i)
