import pandas as pd
import pytest

from www.src.functions import detect_label_column


def _df(columns):
    return pd.DataFrame(columns=columns)


def test_single_matching_label_column_any_language():
    survey = _df(['type', 'name', 'label::Ukrainian (uk)', 'hint::Ukrainian (uk)'])
    choices = _df(['list_name', 'name', 'label::Ukrainian (uk)'])

    assert detect_label_column(survey, choices) == 'label::Ukrainian (uk)'


def test_bare_label_column_no_language_suffix():
    survey = _df(['type', 'name', 'label'])
    choices = _df(['list_name', 'name', 'label'])

    assert detect_label_column(survey, choices) == 'label'


def test_multiple_label_columns_prefers_english():
    survey = _df(['type', 'name', 'label::English', 'label::Ukrainian', 'label::Russian'])
    choices = _df(['list_name', 'name', 'label::English', 'label::Ukrainian', 'label::Russian'])

    assert detect_label_column(survey, choices) == 'label::English'


def test_multiple_label_columns_no_english_falls_back_to_default_language():
    survey = _df(['type', 'name', 'label::Ukrainian (uk)', 'label::Russian (ru)'])
    choices = _df(['list_name', 'name', 'label::Ukrainian (uk)', 'label::Russian (ru)'])
    settings = pd.DataFrame({'default_language': ['Ukrainian (uk)']})

    assert detect_label_column(survey, choices, tool_settings=settings) == 'label::Ukrainian (uk)'


def test_no_common_label_column_raises_value_error():
    survey = _df(['type', 'name', 'label::Ukrainian'])
    choices = _df(['list_name', 'name', 'label::Russian'])

    with pytest.raises(ValueError):
        detect_label_column(survey, choices)


def test_unresolvable_multiple_languages_raises_value_error():
    survey = _df(['type', 'name', 'label::Ukrainian', 'label::Russian'])
    choices = _df(['list_name', 'name', 'label::Ukrainian', 'label::Russian'])
    # no tool_settings passed, no English column present -> can't pick one
    with pytest.raises(ValueError):
        detect_label_column(survey, choices)
