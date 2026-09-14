import pandas as pd
import pytest

from www.src.functions import disaggregation_creator


def _build_daf_final():
    return pd.DataFrame([{
        'ID': 1,
        'variable': 'items',
        'variable_label': 'Items',
        'calculation': None,
        'func': 'select_multiple',
        'admin': 'Overall',
        'disaggregations': None,
        'disaggregations_label': None,
        'join': None,
        'datasheet': 'main',
        'q.type': 'select_multiple',
    }])


def _build_data(values):
    main = pd.DataFrame({
        'items': values,
        'Overall': [' Overall'] * len(values),
        'overall': [' Overall'] * len(values),
    }, dtype=object)
    return {'main': main}


def _build_empty_tool():
    # 'items' is deliberately absent from tool_survey/tool_choices, so
    # disaggregation_creator skips label-mapping and returns raw option
    # codes - exactly what we need to inspect the split/explode step in
    # isolation.
    tool_survey = pd.DataFrame(columns=['name', 'q.type'])
    tool_choices = pd.DataFrame(columns=['list_name', 'name', 'label::English'])
    return tool_survey, tool_choices


def _option_counts(result):
    table = result[0][0]
    return table.set_index('option')['unweighted_count'].to_dict()


def test_default_delimiter_still_splits_on_space():
    daf_final = _build_daf_final()
    data = _build_data(['a b', 'a', 'b c'])
    tool_survey, tool_choices = _build_empty_tool()

    result = disaggregation_creator(
        daf_final, data, {}, tool_choices, tool_survey,
        label_colname='label::English', check_significance=False,
    )

    assert _option_counts(result) == {'a': 2.0, 'b': 2.0, 'c': 1.0}


def test_custom_delimiter_splits_correctly():
    daf_final = _build_daf_final()
    data = _build_data(['a|b', 'a', 'b|c'])
    tool_survey, tool_choices = _build_empty_tool()

    result = disaggregation_creator(
        daf_final, data, {}, tool_choices, tool_survey,
        label_colname='label::English', check_significance=False,
        sm_delimiter='|',
    )

    assert _option_counts(result) == {'a': 2.0, 'b': 2.0, 'c': 1.0}


def test_wrong_delimiter_does_not_split_and_would_corrupt_the_table():
    # Demonstrates the bug this task fixes: splitting on the wrong
    # delimiter leaves each combination as one unrecognized "option"
    # instead of splitting it into individual choices.
    daf_final = _build_daf_final()
    data = _build_data(['a|b', 'a', 'b|c'])
    tool_survey, tool_choices = _build_empty_tool()

    result = disaggregation_creator(
        daf_final, data, {}, tool_choices, tool_survey,
        label_colname='label::English', check_significance=False,
        sm_delimiter=' ',
    )

    options = set(_option_counts(result).keys())
    assert options == {'a|b', 'a', 'b|c'}
