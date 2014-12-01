# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2014.                                 |
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
# | Authors: Rahul Priyadarshi                                               |
# +--------------------------------------------------------------------------+

import datetime
from django.utils import six
from ibm_db_django import schemaEditor

class InformixSchemaEditor(schemaEditor.DB2SchemaEditor):
    
    sql_create_fk = "ALTER TABLE %(table)s ADD CONSTRAINT FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%(to_column)s) CONSTRAINT %(name)s"
    sql_create_unique = "ALTER TABLE %(table)s ADD CONSTRAINT UNIQUE (%(columns)s) CONSTRAINT %(name)s"
    sql_create_column = "ALTER TABLE %(table)s ADD (%(column)s %(definition)s)"
    sql_alter_column_not_null = "MODIFY %(column)s %(type)s NOT NULL"
                
    @property
    def sql_create_pk(self):
        # Constraint name at the end
        return "ALTER TABLE %(table)s ADD CONSTRAINT PRIMARY KEY (%(columns)s) CONSTRAINT %(name)s"
            
    def prepare_default(self, value):
        if isinstance(value, datetime.datetime):
            value= 'CURRENT YEAR TO FRACTION(5)'
        else:
            value= super(InformixSchemaEditor, self).prepare_default(value)
        return value

    def add_field(self, model, field):
        """
        No SET NULL in informix => ibm_db_django version of the 
        method won't work.
        Call django default.
        """
        super(schemaEditor.DB2SchemaEditor, self).add_field(model, field)
                
    def _reorg_tables(self):
        pass
