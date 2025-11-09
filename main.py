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
  dbase.new_table("stores", STORE_DB_COLUMNS)
  dbase.insert_row("stores", {'StoreID':"0", 'Name':"Weis"})
  dbase.insert_row("stores", {'StoreID':"1", 'Name':"Lidl"})
  dbase.insert_row("stores", {'StoreID':"2", 'Name':"Walmart"})
  dbase.insert_row("stores", {'StoreID':"3", 'Name':"Target"})
  dbase.insert_row("stores", {'StoreID':"4", 'Name':"Giant"})
  dbase.insert_row("stores", {'StoreID':"5", 'Name':"Price Right"})
  dbase.insert_row("stores", {'StoreID':"6", 'Name':"Aldi"})
  dbase.insert_row("stores", {'StoreID':"7", 'Name':"7-Eleven"})
  dbase.insert_row("stores", {'StoreID':"8", 'Name':"Rutters"})
  dbase.insert_row("stores", {'StoreID':"9", 'Name':"Turkey Hill"})
  dbase.new_table("store_0", APPROVED_DB_COLUMNS)
  dbase.new_table("store_1", APPROVED_DB_COLUMNS)
  dbase.new_table("store_2", APPROVED_DB_COLUMNS)
  dbase.new_table("store_3", APPROVED_DB_COLUMNS)
  dbase.new_table("store_4", APPROVED_DB_COLUMNS)
  dbase.new_table("store_5", APPROVED_DB_COLUMNS)
  dbase.new_table("store_6", APPROVED_DB_COLUMNS)
  dbase.new_table("store_7", APPROVED_DB_COLUMNS)
  dbase.new_table("store_8", APPROVED_DB_COLUMNS)
  dbase.new_table("store_9", APPROVED_DB_COLUMNS)
  #dbase.insert_row("store_0", {'Barcode':"0028400020008", 'Name':"Test Product"})
  
  #dbase.delete_table("store_0")
  #dbase.delete_table("stores")
  tlist = clean_str_arr(dbase.list_tables(), ['(', ')', ','])
  print(tlist)