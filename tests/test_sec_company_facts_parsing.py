from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from finflow.sec.models import SecCompanyFacts, SecFinancialFact
from finflow.sec.parsing import (
    SecCompanyFactsValidationError,
    parse_company_facts,
)


_MISSING = object()


def _occurrence(**overrides: object) -> dict[str, object]:
    occurrence: dict[str, object] = {
        "start": "2025-01-01",
        "end": "2025-03-31",
        "val": 123456,
        "accn": "0000001234-25-000057",
        "fy": 2025,
        "fp": "Q2",
        "form": "10-Q",
        "filed": "2025-05-02",
        "frame": "CY2025Q1",
    }
    occurrence.update(overrides)
    return occurrence


def _concept(
    *occurrences: object,
    unit: object = "USD",
    label: object = "Revenue",
    description: object = "Revenue from contracts",
) -> dict[str, object]:
    return {
        "label": label,
        "description": description,
        "units": {unit: list(occurrences)},
    }


def _payload(
    facts: object | None = None,
    *,
    cik: object = 320193,
    entity_name: object = "  Apple Inc.  ",
) -> dict[str, Any]:
    if facts is None:
        facts = {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": _concept(
                    _occurrence()
                )
            }
        }
    return {"cik": cik, "entityName": entity_name, "facts": facts}


def test_parses_valid_duration_fact_with_source_context() -> None:
    result = parse_company_facts(_payload(), expected_cik="0000320193")

    assert result == SecCompanyFacts(
        cik="0000320193",
        entity_name="Apple Inc.",
        facts=(
            SecFinancialFact(
                cik="0000320193",
                taxonomy="us-gaap",
                concept="RevenueFromContractWithCustomerExcludingAssessedTax",
                label="Revenue",
                description="Revenue from contracts",
                unit="USD",
                value=Decimal(123456),
                start_date=date(2025, 1, 1),
                end_date=date(2025, 3, 31),
                accession_number="0000001234-25-000057",
                fiscal_year=2025,
                fiscal_period="Q2",
                form="10-Q",
                filed_date=date(2025, 5, 2),
                frame="CY2025Q1",
            ),
        ),
    )


def test_parses_instantaneous_fact_without_start() -> None:
    occurrence = _occurrence(end="2025-03-31", val=987654)
    del occurrence["start"]
    facts = {"us-gaap": {"Assets": _concept(occurrence)}}

    fact = parse_company_facts(_payload(facts), expected_cik=320193).facts[0]

    assert fact.concept == "Assets"
    assert fact.start_date is None


def test_converts_integer_value_exactly() -> None:
    fact = parse_company_facts(_payload(), expected_cik=320193).facts[0]

    assert fact.value == Decimal(123456)


def test_converts_float_value_from_its_string_form() -> None:
    facts = {"custom-taxonomy": {"ExactSourceConcept": _concept(_occurrence(val=0.1))}}

    fact = parse_company_facts(_payload(facts), expected_cik=320193).facts[0]

    assert fact.value == Decimal("0.1")
    assert fact.value.is_finite()


@pytest.mark.parametrize("value", [-10, 0, -0.25])
def test_accepts_negative_and_zero_values(value: int | float) -> None:
    facts = {"us-gaap": {"SourceConcept": _concept(_occurrence(val=value))}}

    fact = parse_company_facts(_payload(facts), expected_cik=320193).facts[0]

    expected = Decimal(value) if isinstance(value, int) else Decimal(str(value))
    assert fact.value == expected


def test_normalizes_top_level_cik() -> None:
    result = parse_company_facts(_payload(cik="320193"), expected_cik=320193)

    assert result.cik == "0000320193"


def test_rejects_payload_cik_mismatch() -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="does not match"):
        parse_company_facts(_payload(cik=789019), expected_cik=320193)


@pytest.mark.parametrize("payload", [None, [], "invalid"])
def test_rejects_non_object_payload(payload: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="payload"):
        parse_company_facts(payload, expected_cik=320193)  # type: ignore[arg-type]


@pytest.mark.parametrize("field_name", ["entityName", "facts"])
def test_rejects_missing_top_level_field(field_name: str) -> None:
    payload = _payload()
    del payload[field_name]

    with pytest.raises(SecCompanyFactsValidationError, match=field_name):
        parse_company_facts(payload, expected_cik=320193)


@pytest.mark.parametrize("entity_name", [None, "", "   ", 123])
def test_rejects_missing_or_invalid_entity_name(entity_name: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="entityName"):
        parse_company_facts(
            _payload(entity_name=entity_name),
            expected_cik=320193,
        )


@pytest.mark.parametrize("facts", [None, [], "invalid"])
def test_rejects_missing_or_invalid_facts(facts: object) -> None:
    payload = _payload({})
    payload["facts"] = facts

    with pytest.raises(SecCompanyFactsValidationError, match="facts"):
        parse_company_facts(payload, expected_cik=320193)


def test_empty_facts_returns_empty_tuple() -> None:
    result = parse_company_facts(_payload({}), expected_cik=320193)

    assert result.facts == ()


@pytest.mark.parametrize("taxonomy", ["", "   ", 123])
def test_rejects_invalid_taxonomy_name(taxonomy: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="taxonomy"):
        parse_company_facts(
            _payload({taxonomy: {}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("taxonomy_value", [None, [], "invalid"])
def test_rejects_non_object_taxonomy_value(taxonomy_value: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="taxonomy value"):
        parse_company_facts(
            _payload({"us-gaap": taxonomy_value}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("concept", ["", "   ", 123])
def test_rejects_invalid_concept_name(concept: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="concept"):
        parse_company_facts(
            _payload({"us-gaap": {concept: _concept(_occurrence())}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("concept_value", [None, [], "invalid"])
def test_rejects_non_object_concept_value(concept_value: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="concept value"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": concept_value}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("label", [None, "", "   ", 123])
def test_rejects_missing_or_invalid_label(label: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="label"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(label=label)}}),
            expected_cik=320193,
        )


def test_maps_blank_description_to_none() -> None:
    facts = {"us-gaap": {"SourceConcept": _concept(_occurrence(), description="  ")}}

    fact = parse_company_facts(_payload(facts), expected_cik=320193).facts[0]

    assert fact.description is None


def test_preserves_nonblank_description_source_value() -> None:
    facts = {
        "us-gaap": {
            "SourceConcept": _concept(
                _occurrence(),
                description="  Source description  ",
            )
        }
    }

    fact = parse_company_facts(_payload(facts), expected_cik=320193).facts[0]

    assert fact.description == "  Source description  "


@pytest.mark.parametrize("description", [None, 123])
def test_rejects_non_string_description(description: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="description"):
        parse_company_facts(
            _payload(
                {"us-gaap": {"SourceConcept": _concept(description=description)}}
            ),
            expected_cik=320193,
        )


@pytest.mark.parametrize("units", [None, [], "invalid"])
def test_rejects_missing_or_non_object_units(units: object) -> None:
    concept = _concept()
    concept["units"] = units

    with pytest.raises(SecCompanyFactsValidationError, match="units"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": concept}}),
            expected_cik=320193,
        )


def test_empty_units_object_contributes_no_facts() -> None:
    concept = _concept()
    concept["units"] = {}

    result = parse_company_facts(
        _payload({"us-gaap": {"SourceConcept": concept}}),
        expected_cik=320193,
    )

    assert result.facts == ()


@pytest.mark.parametrize("unit", ["", "   ", 123])
def test_rejects_invalid_unit_name(unit: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="unit"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(unit=unit)}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("unit_value", [None, {}, "invalid"])
def test_rejects_non_list_unit_value(unit_value: object) -> None:
    concept = _concept()
    concept["units"] = {"USD": unit_value}

    with pytest.raises(SecCompanyFactsValidationError, match="unit value"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": concept}}),
            expected_cik=320193,
        )


def test_empty_unit_list_contributes_no_facts() -> None:
    result = parse_company_facts(
        _payload({"us-gaap": {"SourceConcept": _concept()}}),
        expected_cik=320193,
    )

    assert result.facts == ()


@pytest.mark.parametrize("occurrence", [None, [], "invalid"])
def test_rejects_non_object_fact_occurrence(occurrence: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="occurrence"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(occurrence)}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("start", [None, "", "2025-1-01", "2025-02-30"])
def test_rejects_invalid_supplied_start(start: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="start"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(start=start))}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("end", [None, "", "2025-3-31", "2025-02-30"])
def test_rejects_invalid_or_missing_end(end: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="end"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(end=end))}}),
            expected_cik=320193,
        )


def test_rejects_start_after_end() -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="start"):
        parse_company_facts(
            _payload(
                {
                    "us-gaap": {
                        "SourceConcept": _concept(
                            _occurrence(start="2025-04-01", end="2025-03-31")
                        )
                    }
                }
            ),
            expected_cik=320193,
        )


@pytest.mark.parametrize(
    "value",
    [True, "123", None, float("nan"), float("inf"), float("-inf")],
)
def test_rejects_invalid_fact_value(value: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="val"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(val=value))}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("accession", [None, "", "320193-25-000057"])
def test_rejects_invalid_or_missing_accession(accession: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="accn"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(accn=accession))}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("fiscal_year", [None, _MISSING])
def test_maps_missing_or_null_fiscal_year_to_none(fiscal_year: object) -> None:
    occurrence = _occurrence(fy=fiscal_year)
    if fiscal_year is _MISSING:
        del occurrence["fy"]

    fact = parse_company_facts(
        _payload({"us-gaap": {"SourceConcept": _concept(occurrence)}}),
        expected_cik=320193,
    ).facts[0]

    assert fact.fiscal_year is None


@pytest.mark.parametrize("fiscal_year", [True, "2025", 2025.0, 0, -1])
def test_rejects_invalid_fiscal_year(fiscal_year: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="fy"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(fy=fiscal_year))}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("fiscal_period", [None, "", "   ", _MISSING])
def test_maps_missing_null_or_blank_fiscal_period_to_none(
    fiscal_period: object,
) -> None:
    occurrence = _occurrence(fp=fiscal_period)
    if fiscal_period is _MISSING:
        del occurrence["fp"]

    fact = parse_company_facts(
        _payload({"us-gaap": {"SourceConcept": _concept(occurrence)}}),
        expected_cik=320193,
    ).facts[0]

    assert fact.fiscal_period is None


def test_rejects_non_string_fiscal_period() -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="fp"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(fp=2))}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("form", [None, "", "   ", 10])
def test_rejects_invalid_or_missing_form(form: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="form"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(form=form))}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("filed", [None, "", "2025-5-02", "2025-02-30"])
def test_rejects_invalid_or_missing_filed_date(filed: object) -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="filed"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(filed=filed))}}),
            expected_cik=320193,
        )


@pytest.mark.parametrize("frame", [None, "", "   ", _MISSING])
def test_maps_missing_null_or_blank_frame_to_none(frame: object) -> None:
    occurrence = _occurrence(frame=frame)
    if frame is _MISSING:
        del occurrence["frame"]

    fact = parse_company_facts(
        _payload({"us-gaap": {"SourceConcept": _concept(occurrence)}}),
        expected_cik=320193,
    ).facts[0]

    assert fact.frame is None


def test_rejects_non_string_frame() -> None:
    with pytest.raises(SecCompanyFactsValidationError, match="frame"):
        parse_company_facts(
            _payload({"us-gaap": {"SourceConcept": _concept(_occurrence(frame=1))}}),
            expected_cik=320193,
        )


def test_allows_repeated_accession_numbers_across_facts() -> None:
    occurrence = _occurrence()
    facts = {
        "us-gaap": {
            "SourceConceptOne": _concept(occurrence),
            "SourceConceptTwo": _concept(occurrence),
        }
    }

    result = parse_company_facts(_payload(facts), expected_cik=320193)

    assert len(result.facts) == 2
    assert result.facts[0].accession_number == result.facts[1].accession_number


def test_ignores_extra_concept_and_occurrence_fields() -> None:
    occurrence = _occurrence(extraOccurrenceField="source value")
    concept = _concept(occurrence)
    concept["extraConceptField"] = "source metadata"

    result = parse_company_facts(
        _payload({"us-gaap": {"SourceConcept": concept}}),
        expected_cik=320193,
    )

    assert len(result.facts) == 1


def test_preserves_provider_traversal_order_and_exact_concept_names() -> None:
    facts = {
        "ifrs-full": {
            "FirstExactConcept": {
                "label": "First",
                "description": "First description",
                "units": {
                    "USD": [_occurrence(val=1), _occurrence(val=2)],
                    "shares": [_occurrence(val=3)],
                },
            },
            "secondExactConcept": _concept(_occurrence(val=4), unit="pure"),
        },
        "custom-Taxonomy": {
            "Third_ExactConcept": _concept(_occurrence(val=5), unit="USD/shares")
        },
    }

    result = parse_company_facts(_payload(facts), expected_cik=320193)

    assert [
        (fact.taxonomy, fact.concept, fact.unit, fact.value)
        for fact in result.facts
    ] == [
        ("ifrs-full", "FirstExactConcept", "USD", Decimal(1)),
        ("ifrs-full", "FirstExactConcept", "USD", Decimal(2)),
        ("ifrs-full", "FirstExactConcept", "shares", Decimal(3)),
        ("ifrs-full", "secondExactConcept", "pure", Decimal(4)),
        ("custom-Taxonomy", "Third_ExactConcept", "USD/shares", Decimal(5)),
    ]
