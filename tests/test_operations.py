from lsmbench.execution.operations import apply_operation


def test_parse_json_array_map_fields():
    context = {
        "DESCRIPTION": (
            '[{"tk":"data-sharing","condition":"Data sharing consent","accepted":true},'
            '{"tk":"terms-conditions","condition":"Terms accepted","accepted":false}]'
        )
    }

    out = apply_operation(
        operation="parse_json_array_map_fields",
        context=context,
        source_paths=["$.DESCRIPTION"],
        parameters={
            "field_map": {
                "tk": "code",
                "condition": "condition_text",
                "accepted": "accepted",
            }
        },
    )

    assert out == [
        {
            "code": "data-sharing",
            "condition_text": "Data sharing consent",
            "accepted": True,
        },
        {
            "code": "terms-conditions",
            "condition_text": "Terms accepted",
            "accepted": False,
        },
    ]