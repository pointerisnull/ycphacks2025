from sql_interface import *

STORE_COLUMNS = "Barcode INT, Name VARCHAR(255), PRIMARY KEY (Barcode)"

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
  dbase = sqlinter("localhost", "hacks2025", "root", "root")
  dbase.connect()
  
  fill_reference_db(dbase, "C:/Users/rockstar/Documents/openfoodfacts.csv")
  
  #dbase.new_table("store_0", STORE_COLUMNS):w
  #dbase.delete_table("store_0")
  tlist = clean_str_arr(dbase.list_tables(), ['(', ')', ','])
  print("QUERY TEST")
  print(dbase.query("fooddb", '0028400020008'))