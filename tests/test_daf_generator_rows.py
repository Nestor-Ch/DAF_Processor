import pandas as pd
import pytest

from www.src.daf_generator import generate_daf_rows


def _tool_survey():
    return pd.DataFrame([
        {'name': 'q_sex', 'q.type': 'select_one', 'label::English': 'Sex'},
        {'name': 'q_age', 'q.type': 'integer', 'label::English': 'Age'},
        {'name': 'q_damage', 'q.type': 'select_multiple', 'label::English': 'Damage'},
        {'name': 'oblast', 'q.type': 'select_one', 'label::English': 'Oblast'},
        {'name': 'age_group', 'q.type': 'select_one', 'label::English': 'Age group'},
    ])


def test_paired_combination_row_count_and_shape():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=['oblast'],
        disaggregations=['age_group'],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    # 1 admin * (1 admin-alone row + 1 disaggregation row) = 2 rows
    assert len(result) == 2
    assert list(result['ID']) == [1, 2]
    admin_alone = result[result['disaggregations'].isna()]
    with_disagg = result[result['disaggregations'].notna()]
    assert len(admin_alone) == 1
    assert admin_alone.iloc[0]['admin'] == 'oblast'
    assert admin_alone.iloc[0]['disaggregations_label'] == 'Overall'
    assert len(with_disagg) == 1
    assert with_disagg.iloc[0]['disaggregations'] == 'age_group'
    assert with_disagg.iloc[0]['disaggregations_label'] == 'Age group'


def test_include_overall_admin_adds_overall_when_absent():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=['oblast'],
        disaggregations=[],
        include_overall_admin=True,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    assert set(result['admin']) == {'Overall', 'oblast'}
    assert len(result) == 2


def test_include_overall_admin_does_not_duplicate_when_already_present():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=['Overall', 'oblast'],
        disaggregations=[],
        include_overall_admin=True,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    assert list(result['admin']).count('Overall') == 1


def test_multiple_admins_and_disaggregations_full_cross():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=['Overall', 'oblast'],
        disaggregations=['q_age', 'age_group'],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    # 2 admins * (1 + 2 disaggregations) = 6 rows
    assert len(result) == 6
    assert list(result['ID']) == [1, 2, 3, 4, 5, 6]


def test_func_derivation_by_kobo_type():
    result = generate_daf_rows(
        dependent_vars=['q_sex', 'q_age', 'q_damage'],
        admins=['Overall'],
        disaggregations=[],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    func_by_var = dict(zip(result['variable'], result['func']))
    assert func_by_var['q_sex'] == 'select_one'
    assert func_by_var['q_age'] == 'numeric'
    assert func_by_var['q_damage'] == 'select_multiple'


def test_custom_admin_not_in_tool_does_not_crash():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=['custom_admin_col'],
        disaggregations=[],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    assert list(result['admin']) == ['custom_admin_col']


def test_custom_disaggregation_not_in_tool_falls_back_to_itself_as_label():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=['Overall'],
        disaggregations=['custom_disagg_col'],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    with_disagg = result[result['disaggregations'].notna()].iloc[0]
    assert with_disagg['disaggregations'] == 'custom_disagg_col'
    assert with_disagg['disaggregations_label'] == 'custom_disagg_col'


def test_empty_dependent_vars_returns_empty_dataframe_with_correct_columns():
    result = generate_daf_rows(
        dependent_vars=[],
        admins=['Overall'],
        disaggregations=['age_group'],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    assert len(result) == 0
    assert list(result.columns) == ['ID', 'variable', 'variable_label', 'calculation', 'func',
                                     'admin', 'disaggregations', 'disaggregations_label', 'join']


def test_no_effective_admins_returns_empty_dataframe():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=[],
        disaggregations=['age_group'],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    assert len(result) == 0


def test_calculation_and_join_always_none():
    result = generate_daf_rows(
        dependent_vars=['q_sex'],
        admins=['Overall'],
        disaggregations=['age_group'],
        include_overall_admin=False,
        tool_survey=_tool_survey(),
        label_colname='label::English',
    )

    assert result['calculation'].isna().all()
    assert result['join'].isna().all()
