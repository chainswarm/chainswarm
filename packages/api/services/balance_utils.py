from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class TableColumn:
    name: str
    type: str
    description: str = ""

@dataclass
class TableInfo:
    name: str
    description: str = ""
    columns: List[TableColumn] = None


def get_table_info(client, database: str, tables: List[str]) -> List[TableInfo]:
    """Get information about the specified tables

    Args:
        client: ClickHouse client instance
        database: Database name
        tables: List of table names

    Returns:
        List of TableInfo objects
    """
    table_info_list = []

    for table in tables:
        table_info = TableInfo(name=table)
        table_info_list.append(table_info)

    return table_info_list


def get_column_info(client, database: str, table_name: str) -> List[TableColumn]:
    """Get column information for a specific table

    Args:
        client: ClickHouse client instance
        database: Database name
        table_name: Table name

    Returns:
        List of TableColumn objects
    """
    query = f"DESCRIBE TABLE {database}.{table_name}"
    result = client.query(query)

    columns = []
    for row in result.result_rows:
        column_name = row[0]
        column_type = row[1]
        columns.append(TableColumn(name=column_name, type=column_type))

    return columns


def format_tables_to_schema(table_info_list: List[TableInfo]) -> Dict[str, Any]:
    """Format table information into a schema dictionary

    Args:
        table_info_list: List of TableInfo objects

    Returns:
        Schema dictionary
    """
    schema = {}

    for table in table_info_list:
        table_schema = {
            "description": table.description,
            "columns": {}
        }

        if table.columns:
            for column in table.columns:
                table_schema["columns"][column.name] = {
                    "type": column.type,
                    "description": column.description
                }

        schema[table.name] = table_schema

    return schema


def format_paginated_response(items, page, page_size, total_items):
    """Format response for paginated endpoints

    Args:
        items: List of items to include in the response
        page: Current page number
        page_size: Number of items per page
        total_items: Total number of items available

    Returns:
        Dictionary with standardized pagination metadata
    """
    total_pages = (total_items + page_size - 1) // page_size if page_size > 0 else 0
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "total_items": total_items
    }
