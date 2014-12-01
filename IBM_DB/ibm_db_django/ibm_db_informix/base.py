# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2009-2013.                                 |
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

"""
Informix IDS backend for Django.
"""

from ibm_db_django import base
from django.db.backends import BaseDatabaseFeatures

from ibm_db_informix import creation
from ibm_db_informix import introspection
from ibm_db_informix import operations
from ibm_db_informix import schemaEditor
    
class DatabaseFeatures( base.DatabaseFeatures ):    
    
    supports_subqueries_in_group_by = False
        
    can_defer_constraint_checks = True
    supports_forward_references = True
    
    supports_regex_backreferencing = False
    
    has_select_for_update = True
    supports_long_model_names = True
    
    ignores_nulls_in_unique_constraints = False
    supports_date_lookup_using_string = False  
    allow_sliced_subqueries = False  
    supports_sequence_reset = False # no reset with serials
        

class DatabaseValidation( base.DatabaseValidation ):  
    pass  


class DatabaseWrapper( base.DatabaseWrapper ):
    
    vendor = 'Informix'
    operators = {
        "exact":        "= %s",
        "iexact":       "= UPPER(%s)",
        "contains":     "LIKE %s ESCAPE '\\'",
        "icontains":    "LIKE UPPER(%s) ESCAPE '\\'",
        "gt":           "> %s",
        "gte":          ">= %s",
        "lt":           "< %s",
        "lte":          "<= %s",
        "startswith":   "LIKE %s ESCAPE '\\'",
        "endswith":     "LIKE %s ESCAPE '\\'",
        "istartswith":  "LIKE UPPER(%s) ESCAPE '\\'",
        "iendswith":    "LIKE UPPER(%s) ESCAPE '\\'",
    }

    def __init__( self, *args ):
        super( DatabaseWrapper, self ).__init__( *args )
        
        self.creation = creation.DatabaseCreation( self )
        self.introspection = introspection.DatabaseIntrospection( self )        
        self.ops = operations.DatabaseOperations( self ) 
        self.features = DatabaseFeatures( self )    
        self.validation = DatabaseValidation( self )                   

    def get_new_connection(self, conn_params):
        """
        To override fatures.has_bulk_insert incorrectly set 
        in parent for IDS to True.  
        """
        connection = super( DatabaseWrapper, self ).get_new_connection(conn_params)
        self.features.has_bulk_insert = False
        return connection
                
    def check_constraints(self, table_names=None):
        """
        To check constraints, we set constraints to immediate. 
        Then, when, we're done we must ensure they are returned to deferred.
        """
        self.cursor().execute('SET CONSTRAINTS ALL IMMEDIATE')
        self.cursor().execute('SET CONSTRAINTS ALL DEFERRED')

    def disable_constraint_checking(self):
        """
        Disables foreign key checks, primarily for use in adding rows with forward references. 
        Always returns True, to indicate constraint checks need to be re-enabled.
        """
        self.cursor().execute('SET CONSTRAINTS ALL DEFERRED')
        return True

    def enable_constraint_checking(self):
        """
        Re-enable foreign key checks after they have been disabled.
        """
        self.cursor().execute('SET CONSTRAINTS ALL IMMEDIATE')
 
    def schema_editor(self, *args, **kwargs):
        return schemaEditor.InformixSchemaEditor(self, *args, **kwargs)
