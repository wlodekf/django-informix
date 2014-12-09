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

from ibm_db_django import introspection
from django import VERSION as djangoVersion
if djangoVersion >= (1, 6):
    from django.db.backends import FieldInfo

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
        if djangoVersion < (1, 6):
            for desc in cursor.description:
                description.append( [ desc[0].lower(), ] + desc[1:] )
        else:
            for desc in cursor.description:
                description.append(FieldInfo(*[desc[0].lower(), ] + desc[1:]))
        return description


    def get_constraints(self, cursor, table_name):
        """
        Retrieves any constraints or keys (unique, pk, fk, check, index)
        across one or more columns.

        Returns a dict mapping constraint names to their attributes,
        where attributes is a dict with keys:
         * columns: List of columns this covers
         * primary_key: True if primary key, False otherwise
         * unique: True if this is a unique constraint, False otherwise
         * foreign_key: (table, column) of target, or None
         * check: True if check constraint, False otherwise
         * index: True if index, False otherwise.

        Some backends may return special constraint names that don't exist
        if they don't name constraints of a certain type (e.g. SQLite)
        """        
        constraints = {} 
        if not _IS_JYTHON:
            
            # CHECK
            
            schema = cursor.connection.get_current_schema()
            sql= """SELECT s.constrname, c.colname 
                      FROM systables t, 
                           sysconstraints s, 
                           syscoldepend d, 
                           syscolumns c 
                     WHERE t.tabname='%(table)s' AND 
                           t.tabid = s.tabid AND 
                           s.constrtype = 'C' AND 
                           s.constrid = d.constrid AND 
                           d.tabid = c.tabid AND 
                           d.colno = c.colno
                 """ % {'table': table_name.lower()}
            cursor.execute(sql)
            for constname, colname in cursor.fetchall():
                if constname not in constraints:
                    constraints[constname] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': False,
                        'foreign_key': None,
                        'check': True,
                        'index': False
                    }
                constraints[constname]['columns'].append(colname.lower())
            
            # UNIQUE
                
            sql= """select s.constrname, c.colname 
                      from systables t, 
                           sysconstraints s, 
                           sysindexes i, 
                           syscolumns c 
                     where t.tabname='%(table)s' and 
                           t.tabid=s.tabid and 
                           s.constrtype='U' and 
                           s.idxname=i.idxname and 
                           i.tabid=t.tabid and 
                           i.tabid=c.tabid and 
                           (i.part1=c.colno or i.part2=c.colno or i.part3=c.colno or i.part4=c.colno or i.part5=c.colno or i.part6=c.colno or i.part7=c.colno or i.part8=c.colno or i.part9=c.colno)
                 """ % {'table': table_name.lower()}
            cursor.execute(sql)
            for constname, colname in cursor.fetchall():
                if constname not in constraints:
                    constraints[constname] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': True,
                        'foreign_key': None,
                        'check': False,
                        'index': True
                    }
                constraints[constname]['columns'].append(colname.lower())
            
            # PRIMARY KEY
        
            for pkey in cursor.connection.primary_keys(None, schema, table_name):
                if pkey['PK_NAME'] not in constraints:
                    constraints[pkey['PK_NAME']] = {
                        'columns': [],
                        'primary_key': True,
                        'unique': False,
                        'foreign_key': None,
                        'check': False,
                        'index': True
                    }
                constraints[pkey['PK_NAME']]['columns'].append(pkey['COLUMN_NAME'].lower())    
            
            # FOREIGN KEY
            
            for fk in cursor.connection.foreign_keys( True, schema, table_name ):
                if fk['FK_NAME'] not in constraints:
                    constraints[fk['FK_NAME']] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': False,
                        'foreign_key': (fk['PKTABLE_NAME'].lower(), fk['PKCOLUMN_NAME'].lower()),
                        'check': False,
                        'index': False
                    }
                constraints[fk['FK_NAME']]['columns'].append(fk['FKCOLUMN_NAME'].lower())
                if fk['PKCOLUMN_NAME'].lower() not in constraints[fk['FK_NAME']]['foreign_key']:
                    fkeylist = list(constraints[fk['FK_NAME']]['foreign_key'])
                    fkeylist.append(fk['PKCOLUMN_NAME'].lower())
                    constraints[fk['FK_NAME']]['foreign_key'] = tuple(fkeylist)
                
            # INDEXES
        
            for index in cursor.connection.indexes( True, schema, table_name ):
                if index['INDEX_NAME'] not in constraints:
                    constraints[index['INDEX_NAME']] = {
                        'columns': [],
                        'primary_key': False,
                        'unique': False,
                        'foreign_key': None,
                        'check': False,
                        'index': True
                    }
                elif constraints[index['INDEX_NAME']]['unique'] : # skip unique index 
                    continue
                elif constraints[index['INDEX_NAME']]['primary_key']: # skip pk index
                    continue
                constraints[index['INDEX_NAME']]['columns'].append(index['COLUMN_NAME'].lower())
                
            return constraints
        