import pandas as pd
import pytest

from www.src.functions import detect_label_column, load_tool_survey


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


def test_load_tool_survey_with_bare_label_column(tmp_path):
    # A minimal/untranslated XLSForm: bare 'label' column, no '::language'
    # suffix. detect_label_column() supports and is unit-tested for this
    # case (see test_bare_label_column_no_language_suffix above), but
    # load_tool_survey is a separate, pre-existing function that its result
    # eventually gets passed into - this proves that path doesn't crash.
    survey_df = pd.DataFrame({
        'type': ['select_one yes_no', 'text'],
        'name': ['q1', 'q2'],
        'label': ['Do you agree?', 'Any comments?'],
    })
    choices_df = pd.DataFrame({
        'list_name': ['yes_no', 'yes_no'],
        'name': ['yes', 'no'],
        'label': ['Yes', 'No'],
    })

    tool_path = tmp_path / 'minimal_tool.xlsx'
    with pd.ExcelWriter(tool_path) as writer:
        survey_df.to_excel(writer, sheet_name='survey', index=False)
        choices_df.to_excel(writer, sheet_name='choices', index=False)

    tool_survey = load_tool_survey(str(tool_path), label_colname='label')

    assert not tool_survey.empty
