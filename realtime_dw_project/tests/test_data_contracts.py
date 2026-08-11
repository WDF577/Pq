import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONTRACT = json.loads((ROOT / "contracts" / "cdc_contracts.json").read_text(encoding="utf-8"))
FLINK_DDL = (ROOT / "flink-sql" / "04_mysql_cdc_to_ods.sql").read_text(encoding="utf-8")
MYSQL_DDL = (ROOT / "scripts" / "create_mysql_business_tables.sql").read_text(encoding="utf-8")


def flink_table_block(table_name):
    match = re.search(
        rf"CREATE TABLE\s+{re.escape(table_name)}\s*\((.*?)\)\s*WITH\s*\((.*?)\);",
        FLINK_DDL,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, f"missing Flink table {table_name}"
    return match.group(1), match.group(2)


def mysql_table_block(table_name):
    match = re.search(
        rf"CREATE TABLE\s+{re.escape(table_name)}\s*\((.*?)\)\s*ENGINE=",
        MYSQL_DDL,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert match, f"missing MySQL table {table_name}"
    return match.group(1)


def normalize_type(value):
    return re.sub(r"\s+", "", value).upper()


def test_contract_covers_every_cdc_entity_once():
    entities = CONTRACT["entities"]
    assert CONTRACT["cross_table_consistency"] == "eventual"
    assert {entity["source_table"] for entity in entities} == {
        "order_info",
        "order_detail",
        "payment_info",
        "refund_info",
        "dim_product_scd2",
    }
    assert len({entity["topic"] for entity in entities}) == len(entities)


def test_contract_fields_keys_and_topics_match_flink_ddl():
    for entity in CONTRACT["entities"]:
        source_columns, source_options = flink_table_block(entity["flink_source_table"])
        sink_columns, sink_options = flink_table_block(entity["flink_sink_table"])

        for field in entity["fields"]:
            expected_type = normalize_type(field["flink_type"])
            for table_name, block in (
                (entity["flink_source_table"], source_columns),
                (entity["flink_sink_table"], sink_columns),
            ):
                column = re.search(
                    rf"^\s*{re.escape(field['name'])}\s+([^,\n]+(?:\([^\n]*\))?)\s*,?\s*$",
                    block,
                    flags=re.IGNORECASE | re.MULTILINE,
                )
                assert column, f"{table_name}.{field['name']} missing"
                assert normalize_type(column.group(1)) == expected_type

        primary_key = ", ".join(entity["primary_key"])
        assert re.search(
            rf"PRIMARY KEY\s*\(\s*{re.escape(primary_key)}\s*\)\s+NOT ENFORCED",
            source_columns,
            flags=re.IGNORECASE,
        )
        assert re.search(
            rf"PRIMARY KEY\s*\(\s*{re.escape(primary_key)}\s*\)\s+NOT ENFORCED",
            sink_columns,
            flags=re.IGNORECASE,
        )
        assert f"'table-name' = '{entity['source_table']}'" in source_options
        assert f"'topic' = '{entity['topic']}'" in sink_options
        assert "'connector' = 'upsert-kafka'" in sink_options


def test_contract_fields_and_keys_exist_in_mysql_ddl():
    for entity in CONTRACT["entities"]:
        block = mysql_table_block(entity["source_table"])
        for field in entity["fields"]:
            assert re.search(
                rf"^\s*{re.escape(field['name'])}\s+",
                block,
                flags=re.IGNORECASE | re.MULTILINE,
            ), f"MySQL {entity['source_table']}.{field['name']} missing"

        for key in entity["primary_key"]:
            assert re.search(rf"\b{re.escape(key)}\b", block, flags=re.IGNORECASE)


def test_mysql_contract_registry_covers_scd2():
    for entity in CONTRACT["entities"]:
        assert re.search(
            rf"\('{re.escape(entity['source_table'])}',\s*1,\s*'BASELINE'",
            MYSQL_DDL,
            flags=re.IGNORECASE,
        ), f"cdc_schema_contract is missing {entity['source_table']}"
