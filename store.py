#!/usr/bin/env python
import cPickle as p
import sqlparser

class Base():
        
     d = {}
    
     def connect(self,database):
          self.db = database
          try:
               f = file(database,'r')
               self.d = p.load(f)
               f.close()
          except:
               f = file(database,'w')
               p.dump(self.d,f)
               f.close()
          return self
    
     def query(self,sql):
          parser = sqlparser.test(sql)
          table = self.d[parser.tables[0]]
          if parser.where[0] == '':
               columns = self.columns(table,parser.columns)
          else:
               columns = self.columns(tb_map(parser.where[0][1:],table),parser.columns)
          return columns

     def columns(self,rowset,column):
          if column == '*':
               return rowset
          else:
               return map(lambda x:x.fromkeys(column),rowset)
    
     def execute(self):
          pass

     def insert(self,data,table):
          if table not in self.d:
               self.d[table] = []
          self.d[table].append(data)
          return self

     def close(self):
          
          f = file(self.db,'w')
          p.dump(self.d,f)
          f.close()
          return None

def tb_lambda(condition,element):
    if condition[1] == "=":
        return element.get(condition[0]) == condition[2] and True or False

def tb_map(conditions,seq):
    r = []
    for i in seq:
        saved = []
        for k in conditions:
             saved.append(tb_lambda(k,i))
        if reduce(lambda  x,y:x&y,saved):
            r.append(i)
    return r
