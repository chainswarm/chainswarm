-- Balance Transfers Schema - Main File
-- This file includes all schema parts in the correct order
-- Split into multiple files to avoid ClickHouse query length limitations

-- Include core tables and indexes
-- @include schema_part1_core.sql

-- Include basic views
-- @include schema_part2_basic_views.sql

-- Include behavior profiles view
-- @include schema_part3_behavior_profiles.sql

-- Include classification view
-- @include schema_part4_classification.sql

-- Include suspicious activity view
-- @include schema_part5_suspicious_activity.sql

-- Include relationship and activity patterns views
-- @include schema_part6_relationships_activity.sql