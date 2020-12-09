from stage_tables import stage_table_creation,stage_table_load
from final_tables import optimized_table_creation_load

if __name__=="__main__":
    stage_table_creation()
    stage_table_load()
    optimized_table_creation_load()