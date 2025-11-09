from sql_interface import *

APPROVED_DB_COLUMNS = "Barcode VARCHAR(255), Name VARCHAR(255), PRIMARY KEY (Barcode)"
STORE_DB_COLUMNS = "StoreID VARCHAR(255), Name VARCHAR(255), PRIMARY KEY (StoreID)"

def fill_reference_db(database, file_path):
  my_columns = {
    'code': 'VARCHAR(255) PRIMARY KEY',
    'product_name': 'MEDIUMTEXT'
  }
  
  # 2. Call the import function, setting skip_header=True
  database.import_large_tsv(
    filename=file_path,
    tablename='eanref',
    columns_dict=my_columns
  )
 
if __name__ == "__main__":
  dbase = SQLInterface("localhost", "hacks2025", "root", "root")
  dbase.connect()
  
  #fill_reference_db(dbase, "C:/Users/rockstar/Documents/openfoodfacts.csv")
  
  #print(dbase.query("store_0", "Name", "Barcode", "0028400020008"))
  #dbase.new_table("stores", STORE_DB_COLUMNS)
  #dbase.new_table("store_0", APPROVED_DB_COLUMNS)
  #dbase.insert_row("store_0", {'Barcode':"0028400020008", 'Name':"Test Product"})
  
  #print(dbase.get_table_as_json_payload("store_0"))
  #print(dbase.get_table_as_json_payload("stores"))
  
  #dbase.delete_table("store_0")
  #dbase.delete_table("stores")
  #tlist = clean_str_arr(dbase.list_tables(), ['(', ')', ','])
  #print(dbase.query("eanref", '0028400020008'))