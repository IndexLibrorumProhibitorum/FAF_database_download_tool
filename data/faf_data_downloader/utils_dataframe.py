from typing import Any, Dict, List, Optional
import pandas as pd

def jsonapi_to_dataframe(
    data: List[Dict[str, Any]],
    flatten_relationships: bool = True,
) -> pd.DataFrame:
    """
    Converts JSON:API `data` list into a pandas DataFrame.

    - Flattens attributes into top-level columns
    - Adds `id` column
    - Optionally flattens relationship IDs
    """

    rows: List[Dict[str, Any]] = []

    for item in data:
        row: Dict[str, Any] = {}

        # ID
        row["id"] = item.get("id")

        # Attributes
        attributes = item.get("attributes", {})
        for key, value in attributes.items():
            row[key] = value

        # Relationships
        if flatten_relationships:
            relationships = item.get("relationships", {})

            for rel_name, rel_value in relationships.items():
                rel_data = rel_value.get("data")

                if isinstance(rel_data, dict):
                    # Single relationship
                    row[f"{rel_name}_id"] = rel_data.get("id")

                elif isinstance(rel_data, list):
                    # Many relationship
                    row[f"{rel_name}_ids"] = [
                        entry.get("id") for entry in rel_data
                    ]

                else:
                    row[f"{rel_name}_id"] = None

        rows.append(row)

    return pd.DataFrame(rows)

def convert_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        if "Time" in column or column.endswith("_at"):
            try:
                df[column] = pd.to_datetime(df[column])
            except Exception:
                pass
    return df