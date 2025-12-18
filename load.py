from tqdm import tqdm
from dw import DW




def load(dw:DW, transform_sources:dict[str, list[dict]]):
    '''Recieves the result of the transform function and loads all the data into the duckdb data warehouse'''

    print("\n\n  --- Starting load... ---  ")

    for table_name, table_content in transform_sources.items(): 
        table_obj = dw.get_table(table_name)

        for row in tqdm(table_content, desc=table_name, total=len(table_content)): 

            try: 
                table_obj.insert(row)
            except Exception as exc: 
                print(f"There was a problem adding row {row} into table {table_name}", 1)
                print(exc)
                break

        print(f"All elements from {table_name} inserted successfully into the database\n")

    print("  --- Loading finished ---  ")
    dw.conn_duckdb.commit()


