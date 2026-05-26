from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Any]:
    """
    Detects schema drift between expected and actual schemas.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.

    Returns:
        Dict[str, Any]: A dictionary containing new columns, removed columns, type changes, and drift severity.
    """
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: actual_schema[k] for k in expected_schema if expected_schema[k]!= actual_schema.get(k)}
    has_drift = bool(new_columns or removed_columns or type_changes)

    drift_severity = 'NONE'
    if new_columns:
        if all('null' in v for v in new_columns.values()):
            drift_severity = 'LOW'
        else:
            drift_severity = 'HIGH'
    if removed_columns:
        drift_severity = 'BREAKING'
    if type_changes:
        drift_severity = 'HIGH'

    return {
        'new_columns': new_columns,
       'removed_columns': removed_columns,
        'type_changes': type_changes,
        'has_drift': has_drift,
        'drift_severity': drift_severity
    }

def decide_action(drift_report: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Decides the action to take for each column based on the drift report.

    Args:
        drift_report (Dict[str, Any]): The drift report.

    Returns:
        Dict[str, Dict[str, str]]: A dictionary containing the action, reason, and risk level for each column.
    """
    decisions = {}
    for column, dtype in drift_report['new_columns'].items():
        if dtype =='string':
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable string column', 'risk_level': 'LOW'}
        elif dtype in ['float', 'double']:
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': 'New float/numeric column', 'risk_level': 'HIGH'}
        else:
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': f'New {dtype} column', 'risk_level': 'LOW'}

    for column in drift_report['removed_columns']:
        decisions[column] = {'action': 'HALT','reason': 'Removed column', 'risk_level': 'BREAKING'}

    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, str]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    """
    Applies the schema evolution decisions to the DataFrame.

    Args:
        spark_df (DataFrame): The PySpark DataFrame.
        decisions (Dict[str, Dict[str, str]]): The decisions to apply.
        updated_schema (Dict[str, str]): The updated schema.

    Returns:
        Tuple[DataFrame, List[str]]: The evolved DataFrame and a list of migration notes.
    """
    migration_notes = []
    for column, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(column)
        elif decision['action'] == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f'{column}_anomaly', spark_df[column].isNull().cast('boolean'))
            migration_notes.append(f"Anomaly flag added for column {column}.")
        else:
            migration_notes.append(f"{decision['action']} action taken for column {column}.")

    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Any]:
    """
    Handles schema drift by detecting, deciding, and applying schema evolution.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.
        spark_df (DataFrame, optional): The PySpark DataFrame. Defaults to None.

    Returns:
        Dict[str, Any]: The full evolution report.
    """
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    if not drift_report['has_drift']:
        print("No schema drift detected.")
        return drift_report

    decisions = decide_action(drift_report)
    if spark_df:
        spark_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        drift_report['migration_notes'] = migration_notes

    print("Schema drift detected. Actions taken:")
    for column, details in decisions.items():
        print(f"{column}: {details['action']} ({details['reason']})")
    print(f"Drift severity: {drift_report['drift_severity']}")

    return drift_report
