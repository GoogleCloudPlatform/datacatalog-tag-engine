from enum import Enum
 
class ConfigType(Enum):
    STATIC_TAG_ASSET = 'static_asset_configs'
    DYNAMIC_TAG_TABLE = 'dynamic_table_configs'
    DYNAMIC_TAG_COLUMN = 'dynamic_column_configs' 
    SENSITIVE_TAG_COLUMN = 'sensitive_column_configs'
    GLOSSARY_TAG_ASSET = 'glossary_asset_configs'
    TAG_EXPORT = 'export_configs'
    TAG_IMPORT = 'import_configs'
    TAG_RESTORE = 'restore_configs'
    ENTRY_CREATE = 'entry_configs'