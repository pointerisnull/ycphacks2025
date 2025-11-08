#DATABASE INTERFACE
import mysql.connector
from mysql.connector import Error

class SQLInterface():
  def __init__(self, host, name, username, password):
    self.host = host
    self.database_name = name
    self.username = username
    self.password = password
    self.table_list = []
    
  # establish a connection with the database. If it doesn't exist, create it.
  def connect(self):
    try:
      self.db = mysql.connector.connect(
        host = self.host,
        user = self.username,
        passwd = self.password,
        allow_local_infile=True
      )
      
      if self.db.is_connected():
        self.cursor = self.db.cursor()
        # check if the database exists
        self.cursor.execute("SHOW DATABASES")
        databases = self.cursor.fetchall()
        database_exists = any(db[0] == self.database_name for db in databases)
        if not database_exists:
          # Create the database if it does not exist
          self.cursor.execute(f"CREATE DATABASE {self.database_name}")
          print(f"Database not found! '{self.database_name}' database created successfully.")
          self.cursor.execute(f"USE {self.database_name}")
          print(f"Using {self.database_name}")
          
        else:
          self.cursor.execute(f"USE {self.database_name}")
          print(f"Connected to database '{self.database_name}'")
          print(f"Using {self.database_name}")

    except Error as e:
      print(f"There was a problem connecting to the database: {e}")
  
  # return a list of all tables in database
  def list_tables(self):
    self.cursor.execute(f"SHOW TABLES;")
    return self.cursor.fetchall()
  
  # if table does not exist, create it
  def new_table(self, name, cols):
    tables = self.list_tables()
    if (name) in tables:
      print(f"The table '{name}' already exists.")
    else:
      self.cursor.execute(f"CREATE TABLE {name}({cols});")
      print(f"Created the table '{name}'.")
  
  # if table exists, delete it
  def delete_table(self, name):
        import mysql.connector # Ensure Error is imported for specific exception handling
        from mysql.connector import Error

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return

        # Sanitize and backtick the table name for the SQL query
        # This handles cases where 'name' might be '1751290404' or 'My Table'
        sql_safe_table_name = f"`{name.strip('`')}`" # Strip existing backticks if any, then add them
        
        tables = self.list_tables()
        # list_tables returns unquoted names, so compare with the unquoted version of 'name'
        if (name,) in tables:
            try:
                # Use the backticked table name in the DROP TABLE statement
                self.cursor.execute(f"DROP TABLE {sql_safe_table_name};")
                self.db.commit()
                print(f"Deleted the table '{name}'.")
            except Error as e:
                print(f"Error deleting table '{name}': {e}")
        else:
            print(f"The table '{name}' does not exist...")
            
  # function to import a CSV file and it's contents into a new database table
  def import_csv(self, filename, tablename):
        # Ensure 'csv' module is imported locally within the function if not globally available
        import csv
        from mysql.connector import Error # Import Error for specific exception handling

        if not self.cursor:
            print("Error: Not connected to database. Ensure .connect() was called successfully.")
            return

        try:
            # First pass: Read header to determine table schema
            with open(filename, 'r', encoding='utf-8') as file:
                csv_reader_for_header = csv.reader(file)
                header = [col.strip() for col in next(csv_reader_for_header)] # Read header and strip whitespace

            # Construct column definitions for CREATE TABLE statement
            # This will now retain spaces in column names, using backticks for proper SQL syntax.
            column_definitions_for_create_table = []
            for col_name in header:
                # Retain spaces in column names, but ensure they are properly backticked.
                # No more replacing spaces with underscores here.
                column_definitions_for_create_table.append(f"`{col_name}` VARCHAR(255)") 

            # Join the definitions into a single string for the CREATE TABLE statement
            cols_string_for_new_table_method = ", ".join(column_definitions_for_create_table)

            # Call self.new_table to create the table if it doesn't exist
            self.new_table(tablename, cols_string_for_new_table_method)

            # Second pass: Read data and insert into the table
            with open(filename, 'r', encoding='utf-8') as file:
                csv_reader_for_data = csv.reader(file)
                next(csv_reader_for_data) # Skip header row again for data import

                # Construct the INSERT statement dynamically using the original header names (with spaces)
                # These names must exactly match the column names created in the table (which now have spaces).
                columns_for_insert_statement = ", ".join([f"`{col.strip()}`" for col in header]) # Use original names, backticked
                placeholders = ", ".join(["%s"] * len(header))
                insert_sql = f"INSERT INTO {tablename} ({columns_for_insert_statement}) VALUES ({placeholders})"
                
                data_to_insert = []
                for row in csv_reader_for_data:
                    # Basic validation: ensure row has the expected number of columns
                    if len(row) != len(header):
                        print(f"Warning: Skipping row due to column count mismatch: {row}")
                        continue
                    data_to_insert.append(tuple(row))

                batch_size = 1000 
                for i in range(0, len(data_to_insert), batch_size):
                    batch = data_to_insert[i:i + batch_size]
                    self.cursor.executemany(insert_sql, batch)
                    self.db.commit() # Commit after each batch to save progress and free memory

            print(f"Successfully imported data from '{filename}' into table '{tablename}'.")

        except FileNotFoundError:
            print(f"Error: CSV file not found at '{filename}'")
            self.db.rollback() 
        except Error as e:
            print(f"Error importing CSV into '{tablename}': {e}")
            self.db.rollback() 
        except Exception as e:
            print(f"An unexpected error occurred during CSV import: {e}")
            self.db.rollback()
          # function to import a large TSV file and its contents into a new database table
 
  # function to import a large TSV file and its contents into a new database table
  def import_large_tsv(self, filename, tablename, columns_dict, batch_size=10000):
        """
        Imports specific columns from a large TSV file into a new table.
        This method is optimized for memory by streaming the file line-by-line.

        It ALWAYS reads the first row as a header and finds the columns
        specified in the 'columns_dict' keys.

        *** NEW: This version uses "INSERT IGNORE" to silently skip
        *** rows that violate the PRIMARY KEY (duplicates).

        Args:
            filename (str): The path to the large .tsv file.
            tablename (str): The name of the table to create and insert data into.
            columns_dict (dict): A dictionary mapping column names to MySQL data types.
                                 The keys MUST match the header names.
            batch_size (int, optional): Number of rows to insert per batch. Defaults to 10000.
        """
        import csv
        import os
        import sys
        from mysql.connector import Error # Ensure Error is imported

        # --- Increase the CSV field size limit ---
        try:
            max_int = sys.maxsize
            while True:
                try:
                    csv.field_size_limit(max_int)
                    break
                except OverflowError:
                    max_int //= 2
            print(f"Increased CSV field size limit to {max_int} to handle large fields.")
        except Exception as e:
            print(f"Warning: Could not set a new CSV field size limit. Error: {e}")
        # --- End CSV limit block ---

        # --- 0. Validation ---
        if not self.cursor or not self.db:
            print("Error: Not connected to database. Call .connect() first.")
            return

        if not os.path.exists(filename):
            print(f"Error: Input file not found at '{filename}'")
            return

        if not columns_dict:
            print("Error: 'columns_dict' cannot be empty. Please define your columns.")
            return

        num_columns = len(columns_dict)
        unquoted_tablename = tablename.strip('`')
        sql_safe_tablename = f"`{unquoted_tablename}`"

        print(f"Starting TSV import for '{filename}' into table '{unquoted_tablename}'.")

        try:
            # --- 1. Check if table exists and create it if not ---
            tables_raw = self.list_tables()
            existing_tables = {table[0] for table in tables_raw}

            if unquoted_tablename in existing_tables:
                print(f"Error: Table '{unquoted_tablename}' already exists. Please delete it first if you want to re-import.")
                return
            
            # Create the table definition *directly* from columns_dict
            col_definitions = [f"`{name}` {datatype}" for name, datatype in columns_dict.items()]
            cols_string_for_new_table = ", ".join(col_definitions)
            
            print(f"Table '{unquoted_tablename}' not found. Creating it...")
            self.new_table(unquoted_tablename, cols_string_for_new_table)
            
            # --- 2. Prepare for Batch Insertion ---
            
            # --- THIS IS THE KEY CHANGE ---
            # Use "INSERT IGNORE" to skip duplicates instead of crashing.
            col_names = ', '.join([f"`{name}`" for name in columns_dict.keys()])
            placeholders = ', '.join(['%s'] * num_columns)
            insert_sql = f"INSERT IGNORE INTO {sql_safe_tablename} ({col_names}) VALUES ({placeholders})"
            # --- END KEY CHANGE ---
            
            column_types_ordered = list(columns_dict.values())
            batch_data = []
            total_rows = 0

            # --- 3. Process the Large File (One Pass) ---
            print(f"Streaming and processing '{filename}' (this may take a while)...")
            with open(filename, 'r', encoding='utf-8', newline='') as f:
                reader = csv.reader(f, delimiter='\t')
                
                indices_to_extract = []
                
                try:
                    header = next(reader) # Read the header row
                    header = [h.strip() for h in header]
                    print("Read header row.")
                    print(f"HEADER COLUMNS FOUND: {header}")

                    header_map = {col_name: index for index, col_name in enumerate(header)}
                    
                    for col_name in columns_dict.keys():
                        if col_name in header_map:
                            indices_to_extract.append(header_map[col_name])
                        else:
                            print(f"Error: Required column '{col_name}' not found in the TSV header.")
                            self.db.rollback()
                            return
                    print(f"Columns to import: {list(columns_dict.keys())}")
                    print(f"Will pull data from file indices: {indices_to_extract}")
                except StopIteration:
                    print("Warning: File is empty.")
                    return
                
                # --- Process data rows ---
                for i, row in enumerate(reader):
                    try:
                        if not row:
                            continue
                        
                        data_to_insert_list = []
                        for index in indices_to_extract:
                            data_to_insert_list.append(row[index])
                        
                        ### --- TRUNCATION BLOCK --- ###
                        for col_index, value in enumerate(data_to_insert_list):
                            col_type = column_types_ordered[col_index].upper()
                            try:
                                if col_type.startswith('VARCHAR('):
                                    limit = int(col_type.split('(')[1].split(')')[0])
                                    if len(value) > limit:
                                        data_to_insert_list[col_index] = value[:limit]
                                elif col_type == 'TEXT':
                                    value_bytes = value.encode('utf-8')
                                    if len(value_bytes) > 65535:
                                        truncated_bytes = value_bytes[:65535]
                                        data_to_insert_list[col_index] = truncated_bytes.decode('utf-8', errors='ignore')
                                elif col_type == 'MEDIUMTEXT':
                                    value_bytes = value.encode('utf-8')
                                    if len(value_bytes) > 16777215:
                                        truncated_bytes = value_bytes[:16777215]
                                        data_to_insert_list[col_index] = truncated_bytes.decode('utf-8', errors='ignore')
                            except Exception as trunc_e:
                                print(f"Warning: Could not apply truncation for row {i+1}, col {col_index}. Error: {trunc_e}")
                        
                        data_to_insert = tuple(data_to_insert_list)
                        batch_data.append(data_to_insert)
                        total_rows += 1

                    # --- This block now ONLY catches Python errors during row prep ---
                    except IndexError:
                        print(f"Warning: Skipping malformed row {i+1} (column index out of range). Row: {row}")
                    except Exception as e:
                        print(f"Error preparing row {i+1}: {e}. Row data: {row}")
                    
                    # --- BATCH INSERT LOGIC ---
                    # Moved this outside the inner try/except for clearer DB errors
                    if len(batch_data) >= batch_size:
                        self.cursor.executemany(insert_sql, batch_data)
                        self.db.commit()
                        print(f"  ... Inserted {total_rows} rows (duplicates skipped)...")
                        batch_data = []

            # --- 4. Insert the Final Batch ---
            if batch_data:
                self.cursor.executemany(insert_sql, batch_data)
                self.db.commit()
                print(f"Inserted final batch.")

            print(f"Successfully imported {total_rows} rows from '{filename}' into table '{unquoted_tablename}'.")
            print("Duplicate rows were ignored and not imported.")

        except csv.Error as e:
            print(f"A CSV parsing error occurred: {e}")
            self.db.rollback()
        except Error as e:
            # This will now clearly report any *database* errors
            print(f"A database error occurred during a batch insert: {e}")
            self.db.rollback()
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            self.db.rollback()
  
  def insert_row(self, tablename, data):
        """
        Inserts a new row into the specified table.

        Args:
            tablename (str): The name of the table to insert into.
            data (dict): A dictionary where keys are column names and values are the data for those columns.
                         Example: {'Date': '2025-07-24', 'Value A': '100'}
        """
        from mysql.connector import Error # Ensure Error is imported for specific exception handling

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return

        # Ensure the table name is properly backticked
        sql_safe_tablename = f"`{tablename.strip('`')}`"

        # Check if the table exists
        self.cursor.execute(f"SHOW TABLES LIKE '{tablename.strip('`')}';")
        if not self.cursor.fetchone():
            print(f"Error: Table '{tablename}' does not exist. Cannot insert row.")
            return

        # Dynamically build the INSERT SQL query
        # Backtick column names to handle spaces or special characters
        columns = ", ".join([f"`{col.strip()}`" for col in data.keys()])
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())

        insert_sql = f"INSERT INTO {sql_safe_tablename} ({columns}) VALUES ({placeholders})"

        try:
            self.cursor.execute(insert_sql, values)
            self.db.commit()
            print(f"Successfully inserted a new row into table '{tablename}'.")
        except Error as e:
            print(f"Error inserting row into table '{tablename}': {e}")
            self.db.rollback()
        except Exception as e:
            print(f"An unexpected error occurred during row insertion: {e}")
            self.db.rollback()
  
  def modify_row(self, tablename, primary_key_column, primary_key_value, data):
        """
        Modifies a specified row in the table based on a primary key.

        Args:
            tablename (str): The name of the table to modify.
            primary_key_column (str): The name of the primary key column.
            primary_key_value: The value of the primary key for the row to modify.
            data (dict): A dictionary where keys are column names to update and values are the new data.
                         Example: {'Value A': '150', 'Status': 'Updated'}
        """
        from mysql.connector import Error # Ensure Error is imported for specific exception handling

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return

        # Ensure the table name is properly backticked
        sql_safe_tablename = f"`{tablename.strip('`')}`"

        # Check if the table exists
        self.cursor.execute(f"SHOW TABLES LIKE '{tablename.strip('`')}';")
        if not self.cursor.fetchone():
            print(f"Error: Table '{tablename}' does not exist. Cannot modify row.")
            return

        # Dynamically build the SET part of the UPDATE query
        # Backtick column names to handle spaces or special characters
        set_clauses = ", ".join([f"`{col.strip()}` = %s" for col in data.keys()])
        values = list(data.values())

        # Add the primary key value to the end of the values tuple for the WHERE clause
        values.append(primary_key_value)

        # Ensure primary key column name is backticked
        sql_safe_pk_column = f"`{primary_key_column.strip('`')}`"

        update_sql = f"UPDATE {sql_safe_tablename} SET {set_clauses} WHERE {sql_safe_pk_column} = %s"

        try:
            self.cursor.execute(update_sql, tuple(values))
            self.db.commit()
            if self.cursor.rowcount > 0:
                print(f"Successfully modified row in table '{tablename}' where {primary_key_column} = '{primary_key_value}'.")
            else:
                print(f"No row found or modified in table '{tablename}' where {primary_key_column} = '{primary_key_value}'.")
        except Error as e:
            print(f"Error modifying row in table '{tablename}': {e}")
            self.db.rollback()
        except Exception as e:
            print(f"An unexpected error occurred during row modification: {e}")
            self.db.rollback() 
  
  def get_row(self, tablename, index):
        from mysql.connector import Error 

        if not self.cursor:
            print("DEBUG (get_row_by_index): Not connected to database.")
            return [] 

        if not isinstance(index, int) or index < 0:
            print("DEBUG (get_row_by_index): Invalid index type or value.")
            print("Error: Row index must be a non-negative integer.")
            return []

        sql_safe_tablename = f"`{tablename.strip('`')}`"

        try:
            self.cursor.execute(f"SHOW TABLES LIKE '{tablename.strip('`')}';")
            table_exists_result = self.cursor.fetchone()
            if not table_exists_result:
                print(f"DEBUG (get_row_by_index): SHOW TABLES LIKE returned empty for '{tablename}'.")
                print(f"Error: Table '{tablename}' does not exist in the database.")
                return [] 
            
            select_sql = f"SELECT * FROM {sql_safe_tablename} LIMIT 1 OFFSET {index};"
            self.cursor.execute(select_sql)
            
            row_content = self.cursor.fetchone()

            self.cursor.fetchall() # Consume any remaining results

            if row_content:
                # Store the result of list(row_content) in a temporary variable
                # so we can print its type and value before returning
                final_return_value = list(row_content) 
                return final_return_value
            else:
                print(f"DEBUG (get_row): Row at index {index} was NOT found (row_content is None).")
                print(f"Row at index {index} not found in table '{tablename}'.")
                return None 

        except Error as e:
            print(f"DEBUG (get_row): Database Error: {e}")
            print(f"Error retrieving row from table '{tablename}' at index {index}: {e}")
            try: self.cursor.fetchall()
            except: pass 
            return None
        except Exception as e:
            print(f"DEBUG (get_row_by_index): Unexpected Error: {e}")
            print(f"An unexpected error occurred while retrieving row: {e}")
            try: self.cursor.fetchall()
            except: pass 
            return None
  
  def get_row_count(self, tablename):
        """
        Retrieves the total number of rows in a specified table.

        Args:
            tablename (str): The name of the table.

        Returns:
            int: The total number of rows, or -1 if an error occurs
                 (e.g., table not found).
        """
        from mysql.connector import Error # Ensure Error is imported

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return -1

        # Sanitize table name
        sql_safe_tablename = f"`{tablename.strip('`')}`"
        
        query_sql = f"SELECT COUNT(*) FROM {sql_safe_tablename};"

        try:
            # Execute the query
            self.cursor.execute(query_sql)
            
            # The result will be a single tuple, e.g., (12345,)
            result = self.cursor.fetchone()
            
            # Clear any remaining results
            self.cursor.fetchall()

            if result:
                return result[0]  # Return the count
            else:
                return -1 # Should not happen, but good to check

        except Error as e:
            # This will catch "1146 (42S02): Table '...' doesn't exist"
            print(f"Error getting row count for table '{tablename}': {e}")
            try: self.cursor.fetchall() # Try to clear cursor on error
            except: pass
            return -1
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            try: self.cursor.fetchall()
            except: pass
            return -1 
  
  def get_column_data(self, tablename, column_name):
        # Retrieves all data from a specified column in a table.
        from mysql.connector import Error # Ensure Error is imported for specific exception handling

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return []

        # Ensure table and column names are properly backticked for SQL queries
        sql_safe_tablename = f"`{tablename.strip('`')}`"
        sql_safe_column_name = f"`{column_name.strip('`')}`"

        try:
            # 1. Check if the table exists
            self.cursor.execute(f"SHOW TABLES LIKE '{tablename.strip('`')}';")
            # Consume the result immediately
            if not self.cursor.fetchone():
                print(f"Error: Table '{tablename}' does not exist in the database.")
                return []
            self.cursor.fetchall() # Ensure cursor is clear for next query

            # 2. Check if the column exists within the table
            # Using SHOW COLUMNS FROM is more robust than SELECT ... LIMIT 0 for column existence
            self.cursor.execute(f"SHOW COLUMNS FROM {sql_safe_tablename} LIKE '{column_name.strip('`')}';")
            # Consume the result immediately
            if not self.cursor.fetchone():
                print(f"Error: Column '{column_name}' does not exist in table '{tablename}'.")
                return []
            self.cursor.fetchall() # Ensure cursor is clear for next query

            # 3. Select all data from the specified column
            select_sql = f"SELECT {sql_safe_column_name} FROM {sql_safe_tablename};"
            self.cursor.execute(select_sql)
            
            # Fetch all rows. fetchall() returns a list of tuples, e.g., [('value1',), ('value2',)]
            column_data_tuples = self.cursor.fetchall()

            # --- FIX: Consume the result set explicitly ---
            self.cursor.fetchall() 
            # --- END FIX ---

            # Extract data from the list of tuples into a single flat list
            # Each tuple will only have one element (the column value)
            column_data_list = [item[0] for item in column_data_tuples]
            
            return column_data_list

        except Error as e:
            print(f"Error retrieving column '{column_name}' from table '{tablename}': {e}")
            # Ensure any pending result is cleared on error
            try:
                self.cursor.fetchall()
            except:
                pass 
            return []
        except Exception as e:
            print(f"An unexpected error occurred while retrieving column data: {e}")
            try:
                self.cursor.fetchall()
            except:
                pass 
            return [] 
  
  # return the row index
  def find_row_index(self, tablename, column_name, key_value):
        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return -1

        sql_safe_tablename = f"`{tablename.strip('`')}`"
        sql_safe_column_name = f"`{column_name.strip('`')}`"

        
        # 1. Check if table exists
        self.cursor.execute(f"SHOW TABLES LIKE '{tablename.strip('`')}';")
        if not self.cursor.fetchone():
            print(f"Error: Table '{tablename}' does not exist.")
            return -1
        self.cursor.fetchall() # Clear cursor

        # 2. Check if column exists
        self.cursor.execute(f"SHOW COLUMNS FROM {sql_safe_tablename} LIKE '{column_name.strip('`')}';")
        if not self.cursor.fetchone():
            print(f"Error: Column '{column_name}' does not exist in table '{tablename}'.")
            return -1
        self.cursor.fetchall() # Clear cursor

        # 3. Fetch all values from the specified column to find the index
        # This query implicitly defines the order for indexing in this context.
        # For strict ordering, an ORDER BY clause would be needed, but that
        # would require knowing the column(s) to order by.
        select_sql = f"SELECT {sql_safe_column_name} FROM {sql_safe_tablename};"
        self.cursor.execute(select_sql)
        
        # fetchall returns a list of tuples, e.g., [('value1',), ('value2',)]
        column_values_tuples = self.cursor.fetchall()
        self.cursor.fetchall() # Clear cursor

        # Convert list of tuples to a flat list of values
        column_values = [item[0] for item in column_values_tuples]
        
        ret = -1
        # Search for the key_value in the list
        for i in range(len(column_values)):
            str = column_values[i].strip('`')
            keyval = key_value.strip('`')
            if str == keyval:
                ret = i
                break
        
        return ret
        
  # remove a row given index
  def delete_row_by_index(self, tablename, index):
        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return False

        if not isinstance(index, int) or index < 0:
            print("Error: Row index must be a non-negative integer.")
            return False

        sql_safe_tablename = f"`{tablename.strip('`')}`"

        try:
            # 1. Check if table exists
            self.cursor.execute(f"SHOW TABLES LIKE '{tablename.strip('`')}';")
            if not self.cursor.fetchone():
                print(f"Error: Table '{tablename}' does not exist. Cannot delete row.")
                return False
            self.cursor.fetchall() # Clear cursor

            # 2. Get the primary key column name(s)
            # This assumes a single-column primary key. For composite keys, this logic needs expansion.
            primary_key_column = None
            self.cursor.execute(f"SHOW COLUMNS FROM {sql_safe_tablename} WHERE `Key` = 'PRI';")
            pk_info = self.cursor.fetchone()
            self.cursor.fetchall() # Clear cursor

            if pk_info:
                primary_key_column = pk_info[0] # The first element is the column name
            else:
                print(f"Error: Could not determine primary key for table '{tablename}'. Cannot delete by index.")
                return False

            sql_safe_pk_column = f"`{primary_key_column.strip('`')}`"

            # 3. Fetch all primary key values to determine the one at the given index
            # Again, the order here is not guaranteed without ORDER BY.
            self.cursor.execute(f"SELECT {sql_safe_pk_column} FROM {sql_safe_tablename};")
            pk_values_tuples = self.cursor.fetchall()
            self.cursor.fetchall() # Clear cursor

            if not pk_values_tuples:
                print(f"No rows found in table '{tablename}'. Cannot delete by index {index}.")
                return False

            if index >= len(pk_values_tuples):
                print(f"Error: Index {index} is out of bounds for table '{tablename}' (has {len(pk_values_tuples)} rows).")
                return False

            pk_to_delete = pk_values_tuples[index][0] # Get the PK value at the desired index

            # 4. Delete the row using its primary key
            delete_sql = f"DELETE FROM {sql_safe_tablename} WHERE {sql_safe_pk_column} = %s;"
            self.cursor.execute(delete_sql, (pk_to_delete,))
            self.db.commit()

            if self.cursor.rowcount > 0:
                print(f"Successfully deleted row at conceptual index {index} (Primary Key: '{pk_to_delete}') from table '{tablename}'.")
                return True
            else:
                print(f"No row deleted for Primary Key '{pk_to_delete}' at conceptual index {index} in table '{tablename}'.")
                return False

        except Error as e:
            print(f"Database error deleting row by index: {e}")
            self.db.rollback()
            try: self.cursor.fetchall()
            except: pass
            return False
        except Exception as e:
            print(f"An unexpected error occurred while deleting row by index: {e}")
            self.db.rollback()
            try: self.cursor.fetchall()
            except: pass
            return False  
  
  # export a table as CSV
  def export_table(self, tablename, output_filename):
        """
        Reads all data from a specified MySQL table and exports it to a CSV file.
        """
        
        import csv
        from mysql.connector import Error # Ensure Error is imported for specific exception handling
        import os # For creating directories if needed

        if not self.cursor:
            print("Error: Not connected to database. Ensure .connect() was called successfully.")
            return

        try:
            # Check if the table exists before attempting to read
            self.cursor.execute(f"SHOW TABLES LIKE '{tablename}';")
            table_exists = self.cursor.fetchone()
            if not table_exists:
                print(f"Error: Table '{tablename}' does not exist in the database.")
                return

            # Select all data from the table
            select_sql = f"SELECT * FROM `{tablename}`;"
            self.cursor.execute(select_sql)

            # Get column headers from cursor description
            # cursor.description returns a tuple of (name, type_code, display_size, internal_size, precision, scale, null_ok)
            column_headers = [i[0] for i in self.cursor.description]

            # Fetch all rows from the query result
            rows = self.cursor.fetchall()

            # Ensure output directory exists
            output_dir = os.path.dirname(output_filename)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created directory: {output_dir}")

            # Write data to CSV file
            with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                
                # Write header row
                csv_writer.writerow(column_headers)
                
                # Write data rows
                csv_writer.writerows(rows)

            print(f"Successfully exported data from table '{tablename}' to '{output_filename}'.")

        except Error as e:
            print(f"Error exporting data from table '{tablename}': {e}")
        except Exception as e:
            print(f"An unexpected error occurred during CSV export: {e}")    

  def get_table_as_json_payload(self, tablename):
        """
        Retrieves all data from a table and formats it into the 
        {"payload": [ ... ]} JSON structure.

        Args:
            tablename (str): The name of the table to read from.

        Returns:
            dict: A Python dictionary formatted as requested,
                  or None if an error occurs.
        """
        from mysql.connector import Error # Ensure Error is imported

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return None

        # --- Step 1: Get the Column Names ---
        # We can reuse your existing function for this.
        # This also handily checks if the table exists.
        column_names = self.get_column_names(tablename)
        
        if not column_names:
            # get_column_names() will have already printed an error
            print(f"Aborting: Could not get column names for table '{tablename}'.")
            return None

        # --- Step 2: Fetch All Rows ---
        sql_safe_tablename = f"`{tablename.strip('`')}`"
        query_sql = f"SELECT * FROM {sql_safe_tablename};"

        try:
            self.cursor.execute(query_sql)
            
            # Fetch all rows from the query result
            all_rows = self.cursor.fetchall()
            
            # Clear cursor (follows your established pattern)
            self.cursor.fetchall() 

            # --- Step 3: Zip Column Names with Row Data ---
            # This is the core of the algorithm.
            # We use a list comprehension to create a new dictionary
            # for each row by "zipping" the column_names list 
            # with the row's tuple of values.
            
            payload_list = [dict(zip(column_names, row)) for row in all_rows]

            # --- Step 4: Wrap in the final "payload" dictionary ---
            final_output = {
                "payload": payload_list
            }
            
            return final_output

        except Error as e:
            print(f"Error fetching all rows from table '{tablename}': {e}")
            try: self.cursor.fetchall() # Try to clear cursor on error
            except: pass
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            try: self.cursor.fetchall()
            except: pass
            return None

  def import_dir(self, directory_path):
        """
        Scans a directory for CSV files, and imports them into MySQL tables.
        If a table corresponding to a CSV file already exists, it will be skipped.

        Args:
            directory_path (str): The path to the directory containing CSV files.
        """
        import os # Import os module for directory operations
        
        if not self.cursor:
            print("Error: Not connected to database. Ensure .connect() was called successfully.")
            return

        if not os.path.isdir(directory_path):
            print(f"Error: Directory '{directory_path}' not found or is not a directory.")
            return

        print(f"Scanning directory: '{directory_path}' for new CSV files...")

        try:
            # Get a list of existing tables in the database for quick lookup
            existing_tables_raw = self.list_tables()
            # Convert the list of tuples (('table_name',),) to a set of strings {'table_name'}
            existing_tables = {table[0] for table in existing_tables_raw}
            print(f"Existing tables in database: {', '.join(existing_tables) if existing_tables else 'None'}")

            files_found = 0
            files_imported = 0
            files_skipped = 0

            for filename in os.listdir(directory_path):
                if filename.lower().endswith('.csv'):
                    files_found += 1
                    full_file_path = os.path.join(directory_path, filename)
                    
                    # Derive table name from CSV filename (e.g., "my_data.csv" -> "my_data")
                    base_table_name = os.path.splitext(filename)[0]

                    sql_table_name = f"`{base_table_name}`"
                    
                    # Check if a table with this (unquoted) name already exists in the database's list
                    # Note: existing_tables contains unquoted names from SHOW TABLES
                    if base_table_name in existing_tables:
                        print(f"  Skipping '{filename}': Table '{base_table_name}' already exists.")
                        files_skipped += 1
                    else:
                        print(f"  Found new CSV: '{filename}'. Importing into table '{base_table_name}'...")
                        # Call the existing import_csv function, passing the backticked table name
                        self.import_csv(full_file_path, sql_table_name)
                        files_imported += 1
                        # After successful import, add the unquoted name to existing_tables set
                        existing_tables.add(base_table_name) 

            if files_found == 0:
                print("No CSV files found in the specified directory.")
            else:
                print(f"\nImport process complete:")
                print(f"  Total CSV files found: {files_found}")
                print(f"  New files imported: {files_imported}")
                print(f"  Files skipped (table already exists): {files_skipped}")

        except Exception as e:
            print(f"An error occurred while processing directory '{directory_path}': {e}")

  def get_column_names(self, tablename):
        from mysql.connector import Error # Ensure Error is imported for specific exception handling

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return []

        # Ensure the table name is properly backticked for the SQL query
        sql_safe_table_name = f"`{tablename.strip('`')}`"

        try:
            # Check if the table exists first
            self.cursor.execute(f"SHOW TABLES LIKE '{tablename.strip('`')}';")
            table_exists = self.cursor.fetchone()
            if not table_exists:
                print(f"Error: Table '{tablename}' does not exist in the database.")
                return []

            # Execute a query to get the table's structure/description
            # SELECT * LIMIT 0 is a common way to get column metadata without fetching data
            self.cursor.execute(f"SELECT * FROM {sql_safe_table_name} LIMIT 0;")
            
            # The cursor.description attribute holds metadata about the columns
            # It's a list of tuples, where the first element of each tuple is the column name
            if self.cursor.description:
                column_names = [column[0] for column in self.cursor.description]
                self.cursor.fetchall()
                return column_names
            else:
                print(f"No column information found for table '{tablename}'. It might be empty or malformed.")
                self.cursor.fetchall()
                return []

        except Error as e:
            print(f"Error getting column names for table '{tablename}': {e}")
            return []
        except Exception as e:
            print(f"An unexpected error occurred while getting column names: {e}")
            return []
  
  def query(self, tablename, col, key_col, key):
        from mysql.connector import Error # Ensure Error is imported

        if not self.cursor:
            print("Error: Not connected to database. Call .connect() first.")
            return None

        # Sanitize table name
        sql_safe_tablename = f"`{tablename.strip('`')}`"
        
        # Prepare the query with placeholders
        # We assume the columns are named 'code' and 'product_name'
        query_sql = f"SELECT {col} FROM {sql_safe_tablename} WHERE {key_col} = %s"

        try:
            # Execute the query
            self.cursor.execute(query_sql, (key,))
            
            # Fetch one result
            result = self.cursor.fetchone()
            
            # Clear any remaining results
            self.cursor.fetchall()

            if result:
                return result[0]  # Return the first item in the tuple (the product_name)
            else:
                # No product was found with that code
                return None

        except Error as e:
            print(f"Database error while querying table '{tablename}': {e}")
            try: self.cursor.fetchall() # Try to clear cursor on error
            except: pass
            return None
        except Exception as e:
            print(f"An unexpected error occurred during query: {e}")
            try: self.cursor.fetchall()
            except: pass
            return None
        
def clean_str_arr(strings, chars_to_remove):
  modified_strings = tuple(
    ''.join(c for c in s if c not in chars_to_remove) for s in strings
  )
  return modified_strings

def clean_db(database):
  delims = ['(', ')', ',']
  tables = clean_str_arr(database.list_tables(), delims)
  
  for i in range(len(tables)):
    print(tables[i])
    database.delete_table(tables[i])
  
if __name__ == "__main__":
  dbase = SQLInterface("localhost", "hacks2025", "root", "root")
  dbase.connect()