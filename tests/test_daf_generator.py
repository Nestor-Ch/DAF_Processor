import pandas as pd
import pytest

from www.src.daf_generator import build_group_map


def _survey(rows):
    return pd.DataFrame(rows)


def test_flat_group_single_level():
    survey = _survey([
        {'type': 'begin_group', 'name': 'gr1', 'label::English': 'Group One'},
        {'type': 'select_one yes_no', 'name': 'q1', 'label::English': 'Q1'},
        {'type': 'text', 'name': 'q1_other', 'label::English': 'Q1 other'},
        {'type': 'integer', 'name': 'q2', 'label::English': 'Q2'},
        {'type': 'end_group', 'name': None, 'label::English': None},
    ])

    result = build_group_map(survey, label_colname='label::English')

    assert set(result.keys()) == {'gr1'}
    assert result['gr1']['label'] == 'Group One'
    assert result['gr1']['variables'] == ['q1', 'q2']


def test_nested_group_flattens_into_parent():
    survey = _survey([
        {'type': 'begin_group', 'name': 'outer', 'label::English': 'Outer'},
        {'type': 'select_one yes_no', 'name': 'q_outer', 'label::English': 'Outer Q'},
        {'type': 'begin_group', 'name': 'inner', 'label::English': 'Inner'},
        {'type': 'select_multiple opts', 'name': 'q_inner', 'label::English': 'Inner Q'},
        {'type': 'end_group', 'name': None, 'label::English': None},
        {'type': 'end_group', 'name': None, 'label::English': None},
    ])

    result = build_group_map(survey, label_colname='label::English')

    assert result['outer']['variables'] == ['q_outer', 'q_inner']
    assert result['inner']['variables'] == ['q_inner']


def test_repeat_group_excluded_entirely():
    survey = _survey([
        {'type': 'select_one yes_no', 'name': 'q_main', 'label::English': 'Main Q'},
        {'type': 'begin_repeat', 'name': 'rep1', 'label::English': 'Repeat'},
        {'type': 'begin_group', 'name': 'rep_group', 'label::English': 'Repeat Group'},
        {'type': 'integer', 'name': 'q_rep', 'label::English': 'Repeat Q'},
        {'type': 'end_group', 'name': None, 'label::English': None},
        {'type': 'end_repeat', 'name': None, 'label::English': None},
        {'type': 'begin_group', 'name': 'after', 'label::English': 'After'},
        {'type': 'decimal', 'name': 'q_after', 'label::English': 'After Q'},
        {'type': 'end_group', 'name': None, 'label::English': None},
    ])

    result = build_group_map(survey, label_colname='label::English')

    assert 'rep_group' not in result
    assert set(result.keys()) == {'after'}
    assert result['after']['variables'] == ['q_after']


def test_group_with_no_eligible_questions_omitted():
    survey = _survey([
        {'type': 'begin_group', 'name': 'empty_ish', 'label::English': 'Empty-ish'},
        {'type': 'note', 'name': 'n1', 'label::English': 'A note'},
        {'type': 'text', 'name': 't1', 'label::English': 'A text field'},
        {'type': 'end_group', 'name': None, 'label::English': None},
    ])

    result = build_group_map(survey, label_colname='label::English')

    assert result == {}


def test_group_label_falls_back_to_name_when_missing():
    survey = _survey([
        {'type': 'begin_group', 'name': 'gr_no_label', 'label::English': None},
        {'type': 'select_one yes_no', 'name': 'q1', 'label::English': 'Q1'},
        {'type': 'end_group', 'name': None, 'label::English': None},
    ])

    result = build_group_map(survey, label_colname='label::English')

    assert result['gr_no_label']['label'] == 'gr_no_label'


def test_sibling_groups_tracked_independently():
    survey = _survey([
        {'type': 'begin_group', 'name': 'gr_a', 'label::English': 'A'},
        {'type': 'select_one yes_no', 'name': 'qa', 'label::English': 'QA'},
        {'type': 'end_group', 'name': None, 'label::English': None},
        {'type': 'begin_group', 'name': 'gr_b', 'label::English': 'B'},
        {'type': 'select_one yes_no', 'name': 'qb', 'label::English': 'QB'},
        {'type': 'end_group', 'name': None, 'label::English': None},
    ])

    result = build_group_map(survey, label_colname='label::English')

    assert result['gr_a']['variables'] == ['qa']
    assert result['gr_b']['variables'] == ['qb']


def test_no_label_colname_uses_group_name_as_label():
    survey = _survey([
        {'type': 'begin_group', 'name': 'gr1'},
        {'type': 'select_one yes_no', 'name': 'q1'},
        {'type': 'end_group', 'name': None},
    ])

    result = build_group_map(survey, label_colname=None)

    assert result['gr1']['label'] == 'gr1'
