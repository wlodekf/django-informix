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

from ibm_db_django import operations

if( operations.djangoVersion[0:2] >= ( 1, 4 ) ):
    from django.utils.timezone import is_aware, is_naive, utc 
    from django.conf import settings
    
        
class DatabaseOperations ( operations.DatabaseOperations ):
            
    compiler_module = "ibm_db_informix.compiler"

    def cache_key_culling_sql(self):
        """
        Returns an SQL query that retrieves the first cache key greater than the
        n smallest.

        This is used by the 'db' cache backend to determine where to start
        culling.
        """
        return "SELECT FIRST 1 SKIP %%s cache_key FROM %s ORDER BY cache_key"
    
    def check_aggregate_support( self, aggregate ):
        
        if aggregate.sql_function == 'AVG':
            aggregate.sql_template = '%(function)s(%(field)s)'
        else:
            super( DatabaseOperations, self ).check_aggregate_support( aggregate ) 
    
    def date_extract_sql( self, lookup_type, field_name ):
        if lookup_type.upper() == 'WEEK_DAY':
            return " WEEKDAY(%s)+1 " % ( field_name )
        else:
            return " %s(%s) " % ( lookup_type.upper(), field_name )
     
    def datetime_extract_sql(self, lookup_type, field_name, tzname):
        # Function to extract time zone-aware day, month or day of week from timestamps   
        if settings.USE_TZ:
            hr, min = self._get_utcoffset(tzname)
            if hr < 0:
                field_name = "%s - %s HOURS - %s MINUTES" % (field_name, -hr, -min)
            else:
                field_name = "%s + %s HOURS + %s MINUTES" % (field_name, hr, min)
                
        if lookup_type.upper() == 'WEEK_DAY':
            return " WEEKDAY(%s)+1 " % ( field_name ), []
        else:
            return " %s(%s) " % ( lookup_type.upper(), field_name ), []
             
    def date_trunc_sql( self, lookup_type, field_name ):
        """
        Given a lookup_type of 'year', 'month' or 'day', returns the SQL that
        truncates the given date field field_name to a date object with only
        the given specificity.
        Without casting first TRUNC arg on not null datetimes results in -1260.
        """    
        return "(%s::DATETIME YEAR TO %s::DATETIME YEAR TO MINUTE)" % (field_name, lookup_type.upper())        
    
    # Truncating the time zone-aware timestamps value on the basic of lookup type
    def datetime_trunc_sql( self, lookup_type, field_name, tzname ):
        if settings.USE_TZ:
            hr, min = self._get_utcoffset(tzname)
            if hr < 0:
                field_name = "%s - %s HOURS - %s MINUTES" % (field_name, -hr, -min)
            else:
                field_name = "%s + %s HOURS + %s MINUTES" % (field_name, hr, min)
        return "(%s::DATETIME YEAR TO %s::DATETIME YEAR TO SECOND)" % (field_name, lookup_type.upper()), []
        
    def date_interval_sql( self, sql, connector, timedelta ):
        """
        Implements the interval functionality for expressions.
        """
        minutes, seconds = divmod(timedelta.seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days = str(timedelta.days)
        day_precision = len(days)
        
        if hours or minutes or seconds or timedelta.microseconds:
            fmt = "(%s %s INTERVAL (%s %02d:%02d:%02d.%05d) DAY(%d) TO FRACTION(5))"
            return fmt % (sql, connector, days, hours, minutes, seconds, timedelta.microseconds/10, day_precision)
        
        fmt = "(%s %s INTERVAL (%s) DAY(%d) TO DAY)"
        return fmt % (sql, connector, days, day_precision)
    
    def field_cast_sql(self, db_type, internal_type= None):
        """
        Given a column type (e.g. 'BLOB', 'VARCHAR'), and an internal type
        (e.g. 'GenericIPAddressField'), returns the SQL necessary to cast it
        before using it in a WHERE statement. Note that the resulting string
        should contain a '%s' placeholder for the column being searched against.
        """
        return " %s"
        
    def last_insert_id( self, cursor, table_name, pk_name ):
        return cursor.last_identity_val
    
    def pk_default_value(self):
        """
        Returns the value to use during an INSERT statement to specify that
        the field should use its default value.
        """
        return '0'
        
    def quote_name( self, name ):
        if( name.startswith( "\"" ) & name.endswith( "\"" ) ):
            return name
        
        if( name.startswith( "\"" ) ):
            return "%s\"" % name
        
        if( name.endswith( "\"" ) ):
            return "\"%s" % name
        
        return "\"%s\"" % name
    
    # SQL to return RANDOM number.
    # Within INFORMIX-SQL there is no random number routine.
    # Author: Jonathan Leffler
    # @(#)$Id: random.spl,v 1.2 1997/12/08 19:31:44 johnl Exp $
    # Simple emulation of SRAND and RAND in SPL
    # Using random number generator suggested by C standard (ISO 9899:1990)
    #
    # CREATE PROCEDURE sp_setseed(n INTEGER)
    #       DEFINE GLOBAL seed DECIMAL(10) DEFAULT 1;
    # 
    #       LET seed = n;
    # 
    # END PROCEDURE;
    # CREATE PROCEDURE sp_random() RETURNING INTEGER;
    #      DEFINE GLOBAL seed DECIMAL(10) DEFAULT 1;
    #      DEFINE d DECIMAL(20,0);
    # 
    #      LET d = (seed * 1103515245) + 12345;
    #      -- MOD function does not handle 20-digit values
    #      LET seed = d - 4294967296 * TRUNC(d / 4294967296);
    #  
    #     RETURN MOD(TRUNC(seed / 65536), 32768);
    # 
    # END PROCEDURE;
    
    def random_function_sql( self ):
        """
        Used for random ordering only.
        """
        return "rowid" # "sp_random()"
    
    def regex_lookup(self, lookup_type):
        raise NotImplementedError
        
    def savepoint_create_sql( self, sid ):
        return "SAVEPOINT %s" % sid
    
    def savepoint_commit_sql( self, sid ):
        return "RELEASE SAVEPOINT %s" % sid
    
    def sql_flush(self, style, tables, sequences, allow_cascade=False):
        """
        Serials cannot be reset (backwards).
        """    
        curr_schema = self.connection.connection.get_current_schema().upper()
        sqls = []
        if tables:
            for table in tables:
                sqls.append( style.SQL_KEYWORD( "DELETE" ) + " " + 
                           style.SQL_KEYWORD( "FROM" ) + " " + 
                           style.SQL_TABLE( "%s" % self.quote_name( table ) ) )
        return sqls

    def sequence_reset_by_name_sql(self, style, sequences):
        return []
        
    def sequence_reset_sql( self, style, model_list ):
        return []
    
    def tablespace_sql( self, tablespace, inline = False ):
        return "IN %s" % self.quote_name( tablespace )        
    
    def for_update_sql(self, nowait=False):
        if nowait:
            raise ValueError( "Nowait Select for update not supported " )
        else:
            return 'FOR UPDATE'
