# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2013.                                      |
# +--------------------------------------------------------------------------+
# | This module complies with Django 1.0 and is                              |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Ambrish Bhargava, Tarun Pasrija, Rahul Priyadarshi,             |
# | Wlodek Futrega                                                           |
# +--------------------------------------------------------------------------+

from django.db.models.sql import compiler as base
from ibm_db_django import compiler
        
class SQLCompiler( compiler.SQLCompiler ):
    
    def as_sql( self, with_limits = True, with_col_aliases = False ):
        """
        Ads projection clause to the SELECT statement.
        IDS uses SKIP/FIRST after the SELECT keyword.
        """
        sql, params = super( SQLCompiler, self ).as_sql( False, with_col_aliases )
        
        if with_limits:
            if self.query.low_mark == self.query.high_mark:
                return '', ()
            result= ['SELECT']
            if self.query.low_mark:
                result.append('SKIP %d' % self.query.low_mark)        
            if self.query.high_mark is not None:
                result.append('FIRST %d' % ((self.query.high_mark-self.query.low_mark) or 1))
            result.append(sql_ori[7:])
        
            sql= ' '.join(result)
        
        return sql, params

class SQLInsertCompiler( base.SQLInsertCompiler, SQLCompiler ):
    pass

class SQLDeleteCompiler( base.SQLDeleteCompiler, SQLCompiler ):
    pass

class SQLUpdateCompiler( base.SQLUpdateCompiler, SQLCompiler ):
    pass

class SQLAggregateCompiler( base.SQLAggregateCompiler, SQLCompiler ):
    pass

class SQLDateCompiler( base.SQLDateCompiler, SQLCompiler ):
    pass
