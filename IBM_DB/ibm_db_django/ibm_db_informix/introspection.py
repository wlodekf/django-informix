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

from ibm_db_django import introspection

class DatabaseIntrospection( introspection.DatabaseIntrospection ):
    
    def get_indexes( self, cursor, table_name ):
        """
        Returns a dictionary of indexed fieldname -> infodict for the given
        table, where each infodict is in the format:
            {'primary_key': boolean representing whether it's the primary key,
             'unique': boolean representing whether it's a unique index}

        Only single-column indexes are introspected.
        """        
        indexes = {}
        schema = cursor.connection.get_current_schema()
        multicol_indexes = set()
        for index in cursor.connection.indexes( True, schema, table_name ):
            if index['ORDINAL_POSITION'] > 1:
                multicol_indexes.add(index['INDEX_NAME'])
                        
        for index in cursor.connection.indexes( True, schema, table_name ):
            if index['INDEX_NAME'] in multicol_indexes:
                continue       
            temp = {}
            if ( index['NON_UNIQUE'] ):
                temp['unique'] = False
            else:
                temp['unique'] = True
            temp['primary_key'] = False
            indexes[index['COLUMN_NAME'].lower()] = temp
        
        for index in cursor.connection.primary_keys( True, schema, table_name ):
            indexes[index['COLUMN_NAME'].lower()]['primary_key'] = True
        return indexes
    
    def get_table_description( self, cursor, table_name ):
        "Returns a description of the table, with the DB-API cursor.description interface."        
        qn = self.connection.ops.quote_name
        cursor.execute( "SELECT FIRST 1 * FROM %s" % qn( table_name ) )   
        description = []
        for desc in cursor.description:
            description.append( [ desc[0].lower(), ] + desc[1:] )
        return description
