from omni_source.lineage_parser import extract_field_refs, parse_field_list


def test_extract_field_refs_curly_syntax() -> None:
    refs = extract_field_refs("${orders.total} / NULLIF(${orders.count}, 0)")
    keys = {(ref.view, ref.field) for ref in refs}
    assert ("orders", "total") in keys
    assert ("orders", "count") in keys


def test_extract_field_refs_plain_dot_syntax() -> None:
    refs = extract_field_refs("sum(orders.total) over (partition by users.id)")
    keys = {(ref.view, ref.field) for ref in refs}
    assert ("orders", "total") in keys
    assert ("users", "id") in keys


def test_parse_field_list() -> None:
    refs = parse_field_list(["${orders.total}", "users.country", "badfield"])
    keys = {(ref.view, ref.field) for ref in refs}
    assert ("orders", "total") in keys
    assert ("users", "country") in keys
    assert ("badfield", "") not in keys
