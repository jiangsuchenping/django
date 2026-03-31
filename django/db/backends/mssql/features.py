from django.db.backends.base.features import BaseDatabaseFeatures
from django.utils.functional import cached_property


class DatabaseFeatures(BaseDatabaseFeatures):
    minimum_database_version = (2017,)
    allows_group_by_selected_pks = False
    can_return_columns_from_insert = True
    can_return_rows_from_bulk_insert = True
    has_real_datatype = True
    has_native_uuid_field = True
    has_native_duration_field = False
    has_native_json_field = False
    can_defer_constraint_checks = False
    has_select_for_update = True
    has_select_for_update_nowait = True
    has_select_for_update_of = False
    has_select_for_update_skip_locked = True
    has_select_for_no_key_update = False
    can_release_savepoints = False  # SQL Server doesn't support RELEASE SAVEPOINT
    supports_comments = True
    supports_tablespaces = False
    supports_transactions = True
    can_introspect_materialized_views = False
    can_distinct_on_fields = False
    can_rollback_ddl = False  # DDL is auto-committed in SQL Server
    supports_combined_alters = False
    nulls_order_largest = True
    greatest_least_ignores_nulls = False
    supports_temporal_subtraction = False
    supports_slicing_ordering_in_compound = True
    supports_over_clause = True
    supports_frame_exclusion = False
    supports_aggregate_filter_clause = False
    supports_aggregate_order_by_clause = False
    supports_deferrable_unique_constraints = False
    has_json_operators = False
    supports_update_conflicts = False
    supports_update_conflicts_with_target = False
    supports_covering_indexes = True
    supports_stored_generated_columns = False
    supports_nulls_distinct_unique_constraints = False
    supports_no_precision_decimalfield = True
    can_rename_index = False
    supports_unlimited_charfield = False
    supports_any_value = False
    # SQL Server uses TOP instead of LIMIT
    uses_limit_offset_syntax = False

    @cached_property
    def introspected_field_types(self):
        return {
            **super().introspected_field_types,
            "PositiveBigIntegerField": "BigIntegerField",
            "PositiveIntegerField": "IntegerField",
            "PositiveSmallIntegerField": "SmallIntegerField",
        }

    @cached_property
    def django_test_skips(self):
        skips = {
            "SQL Server does not support these features.": {
                "backends.tests.FkConstraintsTests.test_check_constraints_sql",
            },
        }
        return skips
