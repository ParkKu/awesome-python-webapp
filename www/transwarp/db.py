#!/usr/bin/env python
# -*- coding: utf-8 -*-
#db.py
''''' 设计数据库接口 以方便调用者使用  希望调用者可以通过： 
from transwarp import db 
db.create_engine(user='root',password='123456',database='test',host='127.0.0.1',port=3306) 
然后直接操作sql语句  
users=db.select('select * from user') 
返回一个list 其中包含了所有的user信息。 
其中每一个select和update等 都隐含了自动打开和关闭数据库 这样上层调用就完全不需要关心数据库底层连接 
在一个数据库中执行多条sql语句 可以用with语句实现 
with db.connection(): 
    db.select('....') 
    db.update('....') 
    db.select('....') 
同样如果在一个数据库事务中执行多个SQL语句 也可以用with实现 
with db.transactions(): 
    db.select('....') 
    db.update('....') 
    db.select('....') 

'''

import time, uuid, functools, threading, logging

#Dict object: 重写dict让其通过访问属性的方式访问对应的value
'''--------------以下是Dict类的定义--------------------'''
class Dict(dict):
	'''
	以下是docttest.testmod()会调用作为测试的内容 也就是简单的unittest单元测试
	simple dict but spport access as x.y style

	>>> d1 = Dict()
	>>> d1['x'] = 100
	>>> d1.x
	100
	>>> d1.y = 200
	>>> d1['y'] 
	200
	>>> d2 = Dict(a=1, b=2, c=3)
	>>> d2.c
	'3'
	>>> d2['empty']
	Traceback (most recent call last):
	  ...
	KeyError: 'empty'
	>>> d2.empty
	Traceback (most recent call last):
	  ...
	AttributeError: 'Dict' object has no attribute 'empty'
	>>> d3 = Dict(('a', 'b', 'c'), (1, 2, 3))
	>>> d3.a
	1
	>>> d3.b
	2
	>>> d3.c
	3
	'''

	'''
	@method __init__ 相当于其他语言中的构造函数
	zip()将两个list糅合在一起 例如：
	x = [1,2,3,4,5]
	y = [6,7,8,9,10]
	zip(x,y)-->就得到了[(1,6),(2,7),(3,8),(4,9),(5,10)]
	'''

	#创建实例的时候，第一个参数就是names（可以是list,也可以是tuple，它们是作为一个整体的参数出现的），第二个参数就是values，它们的默认参数都属都是空的tuple  
    #最后一个参数是关键字参数，即“a=1, b=2, c='3'”这样的  
    #特别提醒：【【【 如果是XXX=XXX这样形式的传入参数，只能是关键字参数，从而传值给**kw 】】】
	def __init__(self, names=(), values=(), **kw):  
	''' 自定义Dict是dict的派生类，python中的派生类不会自动调用基类构造函数__init__
		所以要显式调用(用 super 方法更严谨)  
	'''
		super(Dict, self).__init__(**kw) #调用父类的构造方法
		for k, v in zip(names, values):
			self[k] = v

	'''
	@method __getattr__相当于新增加的get方法
	'''
	#python获取属性的方法是d.a这样的形式，但dict本身不支持，所以可以调用__getattr__方法，返回d[a]
	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

	'''
	@method __setattr__相当于新增加的set方法
	'''

	def __setattr__(self, key, value):
		self[key] = value

'''---------------------以上是Dict类的定义--------------------------'''

'''
	@method next_id() uuid4() make a random UUID 得到一个随机UUID
	如果没有传入参数 根据系统当前时间15位和一个随机得到的UUID填充3个0组成一个长度位50的字符串

'''	

def next_id(t = None):
	if t is None:
		t = time.time()
	return '%015d%s000' % (int(t*1000), uuid.uuid4().hex )

'''
	单划线开头的方法名或者属性 只可以在本模块中访问
	@method _profiling 记录sql的运行状态

'''

def _profiling(start, sql=''):
	t = time.time() - start
	if t > 0.1:
		logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
	else:
		logging.info('[PROFILING] [DB] %s: %s' % (t,sql))

#所有的异常默认都继承自Exception 
class DBError(Exception):
	pass

class MultiColumnsError(DBError):
	pass
		

#global engine object 保存着mysql数据库的连接
engine = None


class _Engine(object):
	def __init__(self, connect):
		self._connect = connect
	def connect(self):
		return self._connect() #这里传入的参数是一个函数 将函数调用后 将函数结果返回



def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
	import mysql.connector #导入mysql模块
	global engine #全局变量 说明这个变量在外部定义了
	if engine is not None:
		raise DBError('Engine is already initialized.') #如果已连接 表示连接重复
	params = dict(user=user, password=password, database=database, host= host, port=port) #保存了数据库的连接信息
	defaults = dict(use_unicode=True, charset='utf-8', collation='utf-8_general_ci', autocommit=False) #保存了设置 编码等
	for k, v in defaults.iteritems(): #将defaults和kw中的键值对保存到params中 如果有一个key两边都存在 就保存kw的
		params[k] = kw.pop(k, v) #pop函数会将key为k的键值对删除并返回k对应的value 如果k在kw中不存在 则返回v
	
	#dict.update(dict2)：update()函数把字典dict2的键/值对更新到dict里
	params.update(kw)
	params['buffered'] = True  #增加sql的参数 
	engine = _Engine(lambda: mysql.connector.connect(**params))
	#在这里 (lambda: mysql.connector.connect(**params))返回的是一个函数 不是一个connection的一个对象
	#这一句的目的就是包装，将数据库的连接方法私有化  
    #当调用engine.connect()的时候才真正的进行了数据库的连接，即mysql.connector.connect(**params)。  
    #转了一大圈就为了把mysql.connector.connect(**params)私有化，并方便使用engine.connect()来进行数据库连接  



	#test connection...
	logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))   #转化为十六进制

'''----------------------以上通过engine这个全局变量可以获得一个数据库连接-------------------'''

'''以下对数据库连接以及最基本的操作进行了封装'''

class _LasyConnection(object):
	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			''''' 
            哈哈哈，终于找到你了，就是这一句！ 
            connection=engine.connect() 
            利用create_engine中创建出来的engine实例，而engine实例又是通过_Engine对象创建出来的 
            '''  
			connection = engine.connect() #等价于conn=mysql.connector.connect(**params) 
			logging.info('open connection <%s>...' % hex(id(connection)))
			self.connection = connection
		return self.connection.cursor()  #把游标也对象化，相当于执行cursor=conn.cursor()

	#conn.commit()和conn.rollback()是完全相反的操作，前者是把对数据库的操作提交并更新数据库，后者是撤销所有的数据库操作(不更新数据库)
	def commit(self):
		self.connection.commit()   #相当于conn.commit(),提交更新事务

	def rollback(self):
		#print self.connection
		self.connection.rollback() #相当于conn.rollback(), 回滚事务

	def cleanup(self):
		if self.connection:
			connection = self.connection
			self.connection = None    #关闭连接 
			logging.info('close connection <%s>...' % hex(id(connection)))
			connection.close()        #相当于conn.close()

		
'''接下来解决对于不同的线程数据库链接 应该是不一样的 于是创建了一个变量 是treadlocal的对象'''
# 持有数据库连接的上下文对象
class _DbCtx(threading.local):
	'''Thread local object that holds connection info'''
	def __init__(self):
		self.connection = None             #创建属性
		self.transactions = 0

	def is_init(self):                     #对于每个数据库连接来说
		return not self.connection is None #判断是否已经初始化

	def init(self):                        #创建一个数据库连接对象(实例)
		logging.info('open lazy connection...')
		self.connection = _LasyConnection() #创建一个数据库连接对象(实例)，这样self.connection就可以调用_LasyConnection()类定义的所有方法  
		#print threading.current_thread().name
		#print id(self.connection)
		self.transactions = 0

	def cursor(self):
		'''return cursor'''
		return self.connection.cursor()      #这里调用的是_LasyConnection()类的cursor()方法

	def cleanup(self):
		self.connection.cleanup()            #这里调用的是_LasyConnection()类的cleanup()方法
		self.connection = None

#_db_ctx调用is_init(),init(),cleanup(),cursor()方法时，可以直接调用如_db_ctx.cleanup()  
'''
	但是_DbCtx()类中没写commit()和rollback()方法，若想调用
	_db_ctx.init(),就得在创建了_LasyConnection()类的self.connection
	对象后，用self.connection.commit()来调用_LasyConnection()类的commit()方法	
'''
#由于它继承threading.local 是一个threadlocal对象 所以它对于每一个线程都是不一样的。  
#所以当需要数据库连接的时候就使用它来创建 
_db_ctx = _DbCtx()         #每个线程都有自己的_db_ctx实例(object 对象) 



#通过with语句让数据库链接可以自动创建和关闭
'''
with 语句
 with 后面的语句会返回 _ConnectionCtx 对象 然后调用这个对象的 __enter__方法
 得到返回值并赋予 as 后面的变量 然后执行
 with 下面的语句 执行完毕后 再调用那个对象的__exit__方法

'''

class _ConnectionCtx(object):
	'''
	 _ConnectionCtx object that can open and close connection context.
	 _ConnectionCtx object can nested and only the most outer connection has effect 
	 	with connection():
	 		psaa
	 		with connection():
	 			pass

	'''

	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():            #如果没有连接数据库 
			_db_ctx.init()                   #创建数据库连接对象self.connection
			self.should_cleanup = True
		return self

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def connection():
	'''
	return  _ConnectionCtx object that can be used by 'with' statement : 
	with connection:
		pass

	'''
	return _ConnectionCtx()

#采用装饰器方法  让其能够进行共用同一个数据库连接 
def with_connection(func):
	'''
	#定义with_connection装饰器函数，用于下面select_one(),select_int()等函数
	【【【用于对这些函数执行额外的操作——with操作！】】】
	Decorater for reuse connection 
    @with_connection 
    def foo(*args,**kw): 
    	f1()
    	f2()
    	f3()

    '''
	@functools.wraps(func)
	def wrapper(*args, **kw):
		with connection():               #with 是对函数func 额外做的事情
			return func(*args, **kw)
	return wrapper

#以下是事务处理
class _TransactionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			#need open a connection first
			_db_ctx.init()
			self.should_close_conn = True
		_db_ctx.transactions = _db_ctx.transactions + 1
		logging.info('begin transactions...' if _db_ctx.transactions == 1 else 'join current transactions')
		return self	

	def __exit__(self, exctype, excvalue, traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions - 1
		try:
			if _db_ctx.transactions == 0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()
		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):               #写下面这两个方法，是为了给上面的__exit__()方法调用
		global _db_ctx
		logging.info('commit transaction...')
		try:
			_db_ctx.connection.commit()
			logging.info('commit ok.')
		except:
			logging.warning('commit failed. try rollback...')
			_db_ctx.connection.rollback()
			logging.info('rollback ok.')
			raise

	def rollback(self):
		global _db_ctx
		logging.warning('rollback transactions...')
		_db_ctx.connection.rollback()
		logging.info('rolback ok...')


def transaction():
	return _TransactionCtx()

def with_transaction(func):
	@functools.wraps(func)
	def wrapper(*args, **kw):
		_start = time.time()
		with transaction():
			return func(*args, **kw)
		_profiling(_start)
	return wrapper




def _select(sql, first, *args):           #由first 来决定是select_one还是 select(_all) 
	' execute select SQL and return unique result or list results. '
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s')          #字符串方法：字符替换
	logging.info('SQL: %s, ARGS: %s' % (sql, args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql, args)          #数据库的原始操作（sql是格式化后的字符串'xxx',args是一个tuple）  
		if cursor.description:
			names = [x[0] for x in cursor.description]
		if first:
			values = cursor.fetchone()
			if not values:
				return None
			return Dict(names,values)
		return [Dict(names, x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close()


@with_connection
def select_one(sql, *args):
	return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
	d = _select(sql, True, *args)
	if len(d) != 1:
		raise MultiColumnsError('Expect only one column.')
	return d.values()[0]

@with_connection
def select(sql, *args):
	return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
	global _db_ctx
	cursor = None
	sql = sql.replace('?', '%s')
	logging.info('SQL: %s, ARGS: %s' % (sql, args))
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql, args)
		r = cursor.rowcount
		if _db_ctx.transactions == 0:
			logging.info('auto commit')
			_db_ctx.connection.commit()
		return r 
	finally:
		if cursor:
			cursor.close()


def insert(table, **kw):
	cols ,args = zip(*kw.iteritems())
	sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
	return _update(sql, *args)

def update(sql, *args):
	return _update(sql, *args)

if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine('www-data', 'www-data', 'test')
	update('drop table if exists user')
	update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')	
	import doctest
	doctest.testmod()



	