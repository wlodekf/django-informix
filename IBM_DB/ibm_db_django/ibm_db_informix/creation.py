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

import sys
_IS_JYTHON = sys.platform.startswith( 'java' )

from django.db.models import ForeignKey
from django.db import connections
from ibm_db_django import creation

class DatabaseCreation ( creation.DatabaseCreation ):

    def __init__(self, *args, **kwargs):
        super( DatabaseCreation, self ).__init__( *args, **kwargs )
        
        self.data_types.update({ 
            'AutoField':                    'SERIAL',
            'BinaryField':                  'BLOB',
            'DateTimeField':                'DATETIME YEAR TO FRACTION(5)',
            'FloatField':                   'FLOAT',
            'TimeField':                    'DATETIME HOUR TO SECOND',
        })
        
    def sql_indexes_for_field( self, model, f, style ):
        """Return the CREATE INDEX SQL statements for a single model field.
        IDS - auto creates indexes for foreign key fields.
        """
        if f.db_index and not f.unique and not isinstance(f, ForeignKey):
            return self.sql_indexes_for_fields(model, [f], style)
        else:
            return []
    
    def sql_create_model( self, model, style, known_models = set() ):
        """
        Max VARCHAR size for Informix is 255. 
        Replacing VARCHAR(x) by LVARCHAR(x) for x>255.
        """
        import re
        sql, references = super(creation.DatabaseCreation, self).sql_create_model( model, style, known_models )
        sql= [re.sub('(VARCHAR\((\d+)\))', lambda m: ('L' if int(m.group(2))>255 else '')+m.group(1), field) for field in sql]
        sql= [re.sub('DECIMAL\(38, 30\)', 'DECIMAL(28, 20)', field) for field in sql]
        return sql, references
 
    def sql_for_pending_references(self, model, style, pending_references):
        from django.db.backends.util import truncate_name

        opts = model._meta
        if not opts.managed or opts.proxy or opts.swapped:
            return []
        qn = self.connection.ops.quote_name
        final_output = []
        if model in pending_references:
            for rel_class, f in pending_references[model]:
                rel_opts = rel_class._meta
                r_table = rel_opts.db_table
                r_col = f.column
                table = opts.db_table
                col = opts.get_field(f.rel.field_name).column
                r_name = '%s_refs_%s_%s' % (
                    r_col, col, self._digest(r_table, table))
                final_output.append(style.SQL_KEYWORD('ALTER TABLE') +
                    ' %s ADD CONSTRAINT FOREIGN KEY (%s) REFERENCES %s (%s)%s CONSTRAINT %s;' %
                    (qn(r_table), 
                    qn(r_col), qn(table), qn(col),
                    self.connection.ops.deferrable_sql(),
                    qn(truncate_name(
                        r_name, self.connection.ops.max_name_length()))))
            del pending_references[model]
        return final_output 

    def server_attach_db(self, kwargs):
        return 'sysmaster'
    
    def __create_test_kwargs( self, mode ):
        """
        Eventualy (for db creation only) add mode (transactionality).
        """
        kwargs= super(DatabaseCreation, self).__create_test_kwargs(mode)
        if mode: 
            kwargs['mode']= 'BUFFERED LOG'
        return kwargs


    def create_test_db( self, verbosity = 0, autoclobber = False, serialize=False ):
        """
        Turn off prompts (set autoclobber).
        """
        super(DatabaseCreation, self).create_test_db(verbosity, True, serialize)        
        