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
# | Authors: Rahul Priyadarshi, Wlodek Futrega                               |
# +--------------------------------------------------------------------------+

import datetime
from django.db.models.fields.related import ManyToManyField
from django.utils import six
from ibm_db_django import schemaEditor

class InformixSchemaEditor(schemaEditor.DB2SchemaEditor):
    
    sql_create_fk = "ALTER TABLE %(table)s ADD CONSTRAINT FOREIGN KEY (%(column)s) REFERENCES %(to_table)s (%(to_column)s) CONSTRAINT %(name)s"
    sql_create_unique = "ALTER TABLE %(table)s ADD CONSTRAINT UNIQUE (%(columns)s) CONSTRAINT %(name)s"
    sql_create_column = "ALTER TABLE %(table)s ADD (%(column)s %(definition)s)"
    sql_delete_column = "ALTER TABLE %(table)s DROP %(column)s"    
    sql_modify_column = "MODIFY %(column)s %(definition)s"           
    sql_rename_column = "RENAME COLUMN %(table)s.%(old_column)s TO %(new_column)s"   
    
                            
    def _reorg_tables(self):
        """
        No reorg tables in IDS.
        """
        pass
    
        
    def skip_default(self, field):
        """
        IDS doesn't accept default values for BLOBs, CLOBs
        and implicitly treats these columns as nullable.
        """
        return field.db_type(self.connection) in ('BLOB', 'CLOB')
        
                        
    @property
    def sql_create_pk(self):
        """
        Constraint name at the end
        """
        return "ALTER TABLE %(table)s ADD CONSTRAINT PRIMARY KEY (%(columns)s) CONSTRAINT %(name)s"
                        
                    
    def column_sql(self, model, field, include_default=True, include_unique=True):
        """
        Takes a field and returns its column SQL definition (without CHECK & FK).
        The field must already have had set_attributes_from_name called.
        """        
        db_parameter = field.db_parameters(connection=self.connection)
        sql = db_parameter['type']
        if sql is None:
            return None, None
        if include_default and not self.skip_default(field):
            if (field.default is not None) and field.has_default():
                value = field.get_default()
                value = self.prepare_default(value)
                sql += " DEFAULT %s" % value
        if not field.null and not self.skip_default(field):
            sql += " NOT NULL"
        if field.primary_key:
            sql += " PRIMARY KEY"
        elif field.unique and include_unique:
            sql += " UNIQUE"
        tablespace = field.db_tablespace or model._meta.db_tablespace
        if tablespace and field.unique:
            sql += " %s" % self.connection.ops.tablespace_sql(tablespace, inline=True)
            
        return sql, []
                

    def column_def(self, model, field, include_default=True, include_unique= True):
        """
        Append CHECK to column sql.
        """
        # Get the column's definition (with default)
        definition, params = self.column_sql(model, field, include_default, include_unique)
        # It might not actually have a column behind it
        if definition is None:
            return None, None
        db_params = field.db_parameters(connection=self.connection)
        if db_params['check']:
            definition += " CHECK (%s)" % db_params['check']  
        return definition, params
    
        
    def prepare_default(self, value):
        """
        1. No literals for datetime default - use current instead
        2. Replace ' with '' in strings
        """
        if isinstance(value, datetime.datetime):
            return 'CURRENT YEAR TO FRACTION(5)'
            
        CONVERT_STR= (datetime.datetime, datetime.date, datetime.time, six.string_types)

        if callable(value):
            value = value()

        if isinstance(value, CONVERT_STR):
            value= value.replace("'", "''")
            value = "'%s'" % value
        elif isinstance(value, bool):
            value = '1' if value else '0'
        else:
            value = str(value)
        return value
    
            
    def add_field(self, model, field):
        """
        Creates a field on a model.
        Usually involves adding a column, but may involve adding a
        table instead (for M2M fields)
        
        Informix IDS: 
        1. No DROP DEFAULT clause - can only get rid of default by redefinition
        """
        
        # Special-case implicit M2M tables
        
        if isinstance(field, ManyToManyField) and field.rel.through._meta.auto_created:
            return self.create_model(field.rel.through)
        
        # Get the column's definition (with default)
        definition, params = self.column_def(model, field)
        # It might not actually have a column behind it
        if definition is None:
            return
        
        # Build the SQL and run it
        sql = self.sql_create_column % {
            "table": self.quote_name(model._meta.db_table),
            "column": self.quote_name(field.column),
            "definition": definition,
        }
        self.execute(sql, params)
        
        # Simulate the effect of a one-off default
        # for blobs that do not support default
        if self.skip_default(field) and field.has_default():
            effective_default = self.effective_default(field)
            self.execute('UPDATE %(table)s SET %(column)s=%(default)s' % {
                'table': self.quote_name(model._meta.db_table),
                'column': self.quote_name(field.column),
                'default': self.prepare_default(field.get_default())            
            })
        
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        # IDS: Runs MODIFY (column) without DEFAULT
        # Cannot join with column creation because 
        # when column is not null and has default 
        # existing rows should be populated with default.
        
        if not self.skip_default(field) and field.has_default():
            definition, params = self.column_def(model, field, include_default=False)
            sql = self.sql_alter_column % {
                    "table": self.quote_name(model._meta.db_table),
                    "changes": self.sql_modify_column % {
                                    "column": self.quote_name(field.column),
                                    "definition": definition,
                                }
                }                           
            self.execute(sql, params)
        
        # Add an index, if required
        
        if field.db_index and not field.unique:
            self.deferred_sql.append(
                self.sql_create_index % {
                    "name": self._create_index_name(model, [field.column], suffix=""),
                    "table": self.quote_name(model._meta.db_table),
                    "columns": self.quote_name(field.column),
                    "extra": "",
                }
            )
            
        # Add any FK constraints later
        
        if field.rel and self.connection.features.supports_foreign_keys and field.db_constraint:
            self.deferred_sql.append(self._create_fk_sql(model, field, "_fk_%(to_table)s_%(to_column)s"))
            
        # Reset connection if required
        
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()


    def _alter_many_to_many(self, model, old_field, new_field, strict):
        """
        Skip DB2 version, call original django version.
        """        
        super(schemaEditor.DB2SchemaEditor, self)._alter_many_to_many(model, old_field, new_field, strict)

        
    def column_modify_sql(self, model, field, include_default=True, include_unique=True):
        """
        Construct column MODIFY clause (column redefinition) with or without
        DEFAULT and/or UNIQUE.
        """
        definition, params = self.column_def(model, field, include_default, include_unique)
        return self.sql_modify_column % {
            "column": self.quote_name(field.column),
            "definition": definition,
        }
        
                
    def alter_field(self, model, old_field, new_field, strict=False):
        """
        Skip DB2 version, call original django version.
        """
        super(schemaEditor.DB2SchemaEditor, self).alter_field(model, old_field, new_field, strict)
        
                
    def _alter_field(self, model, old_field, new_field, old_type, new_type, old_db_params, new_db_params, strict=False):
        """
        Actually perform a "physical" (non-ManyToMany) field update.
        
        In IDS column type change and default value manipulation can 
        only be achieved by column redefinition (ALTER TABLE MODIFY).
        This drops FK so it should be restored after last MODIFY.
        """

        # Has unique been removed?
        
        if old_field.unique and (not new_field.unique or (not old_field.primary_key and new_field.primary_key)):
            # Find the unique constraint for this field
            constraint_names = self._constraint_names(model, [old_field.column], unique=True)
            if strict and len(constraint_names) != 1:
                raise ValueError("Found wrong number (%s) of unique constraints for %s.%s" % (
                    len(constraint_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for constraint_name in constraint_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_unique, model, constraint_name))
                
        # Drop any FK constraints, we'll remake them later
        
        fks_dropped = set()
        if old_field.rel and old_field.db_constraint:
            fk_names = self._constraint_names(model, [old_field.column], foreign_key=True)
            if strict and len(fk_names) != 1:
                raise ValueError("Found wrong number (%s) of foreign key constraints for %s.%s" % (
                    len(fk_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for fk_name in fk_names:
                fks_dropped.add((old_field.column,))
                self.execute(self._delete_constraint_sql(self.sql_delete_fk, model, fk_name))
                
        # Drop incoming FK constraints if we're a primary key and things are going
        # to change.
        
        if old_field.primary_key and new_field.primary_key and old_type != new_type:
            for rel in new_field.model._meta.get_all_related_objects(): 
                rel_fk_names = self._constraint_names(rel.model, [rel.field.column], foreign_key=True)
                for fk_name in rel_fk_names:
                    self.execute(self._delete_constraint_sql(self.sql_delete_fk, rel.model, fk_name))
                    
        # Removed an index
        
        if old_field.db_index and not new_field.db_index and not old_field.unique and not (not new_field.unique and old_field.unique):
            # Find the index for this field
            index_names = self._constraint_names(model, [old_field.column], index=True)
            if strict and len(index_names) != 1:
                raise ValueError("Found wrong number (%s) of indexes for %s.%s" % (
                    len(index_names),
                    model._meta.db_table,
                    old_field.column,
                ))
            for index_name in index_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_index, model, index_name))
                
        # Have they renamed the column?
        
        if old_field.column != new_field.column:
            self.execute(self.sql_rename_column % {
                "table": self.quote_name(model._meta.db_table),
                "old_column": self.quote_name(old_field.column),
                "new_column": self.quote_name(new_field.column),
                "type": new_type,
            })
            
        # Next, start accumulating actions to do
        
        actions = []
        null_actions = []
        post_actions = []
        
        # Type change?
        
        if old_type != new_type:
            actions.append((self.column_modify_sql(model, new_field, include_default=False, include_unique=False), []))
                    
        # When changing a column NULL constraint to NOT NULL with a given
        # default value, we need to perform 4 steps:
        #  1. Add a default for new incoming writes
        #  2. Update existing NULL rows with new default
        #  3. Replace NULL constraint with NOT NULL
        #  4. Drop the default again.
        
        # Default change?
        
        old_default = self.effective_default(old_field)
        new_default = self.effective_default(new_field)
        if old_default != new_default:
            actions.append((
                # Set new column default, unique was disabled
                self.column_modify_sql(model, new_field, include_default=new_default is not None, include_unique=False),                                    
                [],
            ))

        # Nullability change?
        
        if old_field.null != new_field.null:
            null_actions.append((
                # Change NULL, unique was disabled
                self.column_modify_sql(model, new_field, include_default=True, include_unique=False),
                [],
            ))
                
        # Only if we have a default and there is a change from NULL to NOT NULL
        
        four_way_default_alteration = (
            new_field.has_default() and
            (old_field.null and not new_field.null)
        )
        
        if actions or null_actions:
            if not four_way_default_alteration:
                # If we don't have to do a 4-way default alteration we can
                # directly run a (NOT) NULL alteration
                actions = actions + null_actions
                
            # Apply those actions
            for sql, params in actions:
                self.execute(
                    self.sql_alter_column % {
                        "table": self.quote_name(model._meta.db_table),
                        "changes": sql,
                    },
                    params,
                )
                
            if four_way_default_alteration:
                
                # Update existing rows with default value
                self.execute(
                    self.sql_update_with_default % {
                        "table": self.quote_name(model._meta.db_table),
                        "column": self.quote_name(new_field.column),
                        "default": "%s",
                    },
                    [new_default],
                )
                
                # Since we didn't run a NOT NULL change before we need to do it now
                for sql, params in null_actions:
                    self.execute(
                        self.sql_alter_column % {
                            "table": self.quote_name(model._meta.db_table),
                            "changes": sql,
                        },
                        params,
                    )
                    
        if post_actions:
            for sql, params in post_actions:
                self.execute(sql, params)
                 
        # Added a unique?
        
        if not old_field.unique and new_field.unique:
            self.execute(self.column_modify_sql(model, new_field, include_default=False, include_unique=True))
            
        # Added an index?
        
        if not old_field.db_index and new_field.db_index and not new_field.unique and not (not old_field.unique and new_field.unique):
            self.execute(
                self.sql_create_index % {
                    "table": self.quote_name(model._meta.db_table),
                    "name": self._create_index_name(model, [new_field.column], suffix=""),
                    "columns": self.quote_name(new_field.column),
                    "extra": "",
                }
            )
            
        # Type alteration on primary key? 
        # Then we need to alter the column referring to us.
        
        rels_to_update = []
        if old_field.primary_key and new_field.primary_key and old_type != new_type:
            rels_to_update.extend(new_field.model._meta.get_all_related_objects())
            
        # Changed to become primary key?
        # Note that we don't detect unsetting of a PK, as we assume another field
        # will always come along and replace it.
        
        if not old_field.primary_key and new_field.primary_key:
            # First, drop the old PK
            constraint_names = self._constraint_names(model, primary_key=True)
            if strict and len(constraint_names) != 1:
                raise ValueError("Found wrong number (%s) of PK constraints for %s" % (
                    len(constraint_names),
                    model._meta.db_table,
                ))
            for constraint_name in constraint_names:
                self.execute(self._delete_constraint_sql(self.sql_delete_pk, model, constraint_name))
                
            # Make the new one
            self.execute(
                self.sql_create_pk % {
                    "table": self.quote_name(model._meta.db_table),
                    "name": self.quote_name(self._create_index_name(model, [new_field.column], suffix="_pk")),
                    "columns": self.quote_name(new_field.column),
                }
            )
            # Update all referencing columns
            rels_to_update.extend(new_field.model._meta.get_all_related_objects())
            
        # Handle our type alters on the other end of rels from the PK stuff above
        
        for rel in rels_to_update:
            self.execute(
                self.sql_alter_column % {
                    "table": self.quote_name(rel.model._meta.db_table),
                    "changes": self.column_modify_sql(rel.model, rel.field, include_default=True, include_unique=True),            
                }
            )
            
        # Rebuild FKs that pointed to us if we previously had to drop them
        
        if old_field.primary_key and new_field.primary_key and old_type != new_type:
            for rel in new_field.model._meta.get_all_related_objects():
                self.execute(self._create_fk_sql(rel.model, rel.field, "_fk"))
                
        # Drop the default if we need to
        # (Django usually does not use in-database defaults)
        
        if not self.skip_default(new_field) and new_field.has_default():
            sql = self.sql_alter_column % {
                "table": self.quote_name(model._meta.db_table),
                "changes": self.column_modify_sql(model, new_field, include_default=False, include_unique=True)            
            }
            self.execute(sql)
            
            
        # Does it have a foreign key?
        # It should be after last ALTER TABLE MODIFY because it drops foreign key!
        
        if new_field.rel and \
           (fks_dropped or (old_field.rel and not old_field.db_constraint)) and \
           new_field.db_constraint:
            self.execute(self._create_fk_sql(model, new_field, "_fk_%(to_table)s_%(to_column)s"))
                        
        # Reset connection if required
        
        if self.connection.features.connection_persists_old_columns:
            self.connection.close()


    def _model_indexes_sql(self, model):
        """
        Return all index SQL statements (field indexes, index_together) for the
        specified model, as a list.
        
        IDS: Don't create indexes for foreign keys as they are created
        automatically on FK constraints creation which is before 
        index creation.
        """
        if not model._meta.managed or model._meta.proxy or model._meta.swapped:
            return []
        output = []
        for field in model._meta.local_fields:
            if field.db_index and not field.unique and (not field.rel or not field.db_constraint):
                output.append(self._create_index_sql(model, [field], suffix=""))

        for field_names in model._meta.index_together:
            fields = [model._meta.get_field_by_name(field)[0] for field in field_names]
            output.append(self._create_index_sql(model, fields, suffix="_idx"))
        return output
