from enum import Enum
 
class ConfigType(Enum):
    DYNAMIC_TAG_TABLE = 'dynamic_table_configs'
    DYNAMIC_TAG_COLUMN = 'dynamic_column_configs' 
    TAG_IMPORT = 'import_configs'
    TAG_EXPORT = 'export_configs'
    TAG_RESTORE = 'restore_configs'