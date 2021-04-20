# Table-specific Constants
REPL_TABLE_NAME = "figgy-config-replication"
REPL_DEST_KEY_NAME = "destination"
REPL_RUN_ENV_KEY_NAME = "env_alias"
REPL_SOURCE_ATTR_NAME = "source"
REPL_NAMESPACE_ATTR_NAME = "namespace"
REPL_TYPE_ATTR_NAME = "type"
REPL_TYPE_APP = "app"
REPL_TYPE_MERGE = "merge"
REPL_USER_ATTR_NAME = "user"

# Audit Table
AUDIT_TABLE_NAME = "figgy-config-auditor"
AUDIT_PARAMETER_KEY_NAME = "parameter_name"
AUDIT_TIME_KEY_NAME = "time"
AUDIT_ACTION_ATTR_NAME = "action"
AUDIT_USER_ATTR_NAME = "user"

AUDIT_PARAMETER_ATTR_TYPE = "type"
AUDIT_PARAMETER_ATTR_VERSION = "version"
AUDIT_PARAMETER_ATTR_DESCRIPTION = "description"
AUDIT_PARAMETER_ATTR_VALUE = "value"
AUDIT_PARAMETER_ATTR_KEY_ID = "key_id"
AUDIT_PARAMETER_ATTR_USER = "user"
AUDIT_IDX_USER_ID = "AuditByUserIdx"

# Cache Table
CACHE_TABLE_NAME = 'figgy-config-cache'
CACHE_PARAMETER_KEY_NAME = "parameter_name"
CACHE_LAST_UPDATED_KEY_NAME = "last_updated"
CACHE_STATE_ATTR_NAME = 'state'

# Config Usage Tracker
CONFIG_USAGE_TABLE_NAME = "figgy-config-usage-tracker"
CONFIG_USAGE_PARAMETER_KEY = "parameter_name"
CONFIG_USAGE_LAST_UPDATED_KEY = "last_updated"
CONFIG_USAGE_USER_KEY = "user"
CONFIG_USAGE_EMPTY_IDX_KEY = "empty_indexable_key"
CONFIG_USAGE_EMPTY_IDX_VALUE = "empty"
CONFIG_USAGE_LAST_UPDATED_ONLY_IDX = "LastUpdateOnlyIdx"
CONFIG_USAGE_USER_LAST_UPDATED_IDX = "UserLastUpdatedIndex"

# User cache table
USER_CACHE_TABLE_NAME = "figgy-user-cache"
USER_CACHE_PARAM_NAME_KEY = "user_name"
USER_CACHE_STATE_ATTR_NAME = "state"
USER_CACHE_LAST_UPDATED_KEY = "last_updated"
USER_CACHE_STATE_DELETED = 'DELETED'
USER_CACHE_STATE_ACTIVE = 'ACTIVE'

# SSM Constants
SSM_SECURE_STRING = "SecureString"
SSM_STRING = "String"
SSM_PUT = 'PutParameter'
SSM_DELETE = 'DeleteParameter'
SSM_GET = 'GetParameter'
SSM_INTELLIGENT_TIERING = 'Intelligent-Tiering'
