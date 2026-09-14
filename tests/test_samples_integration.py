from pathlib import Path

import pandas as pd
import pytest

from www.src.functions import (
    get_sheets_small_data,
    load_tool_choices,
    load_tool_survey,
    check_daf_consistency,
    detect_label_column,
    disaggregation_creator,
)
from www.src.daf_generator import build_group_map, generate_daf_rows

REPO_ROOT = Path(__file__).resolve().parent.parent
SAMPLES = REPO_ROOT / 'samples'
SAMPLES2 = REPO_ROOT / 'samples2'


def _pandas2_compat(df):
    # This sandbox only has pandas 3.0.x available (see the plan's Global
    # Constraints). Under pandas 3.0.x, dtype="str" columns come back as
    # StringDtype instead of the plain object dtype the pinned pandas==2.0.3
    # produces, which makes the .values == comparisons in map_names/
    # map_names_ls raise "Lengths of operands do not match" - a known,
    # already-diagnosed environment gap unrelated to this feature. This
    # normalizes back to object dtype (a no-op under pandas 2.0.3) so these
    # tests exercise this plan's behavior instead of that unrelated gap.
    return df.astype(object)


def _load_tool(tool_path):
    sheets_dat, small_data = get_sheets_small_data(tool_path)
    tool_s = pd.read_excel(tool_path, sheet_name='survey')
    tool_c = pd.read_excel(tool_path, sheet_name='choices')
    tool_settings = small_data.get('settings')
    label_col = detect_label_column(tool_s, tool_c, tool_settings=tool_settings)

    tool_choices = _pandas2_compat(load_tool_choices(tool_path, label_colname=label_col))
    tool_survey = _pandas2_compat(load_tool_survey(tool_path, label_colname=label_col))
    return label_col, tool_choices, tool_survey


def _build_daf_final(rows, tool_survey):
    daf = pd.DataFrame(rows)
    daf = daf.merge(tool_survey[['name', 'q.type']], left_on='variable', right_on='name', how='left')
    daf['q.type'] = daf['q.type'].fillna('select_one')
    return daf


@pytest.mark.skipif(
    not (SAMPLES / 'MSNA_2023_Questionnaire_Final_CATI_cleaned.xlsx').exists()
    or not (SAMPLES / 'UKR2308_MSNA_clean_data_230829_full.xlsx').exists(),
    reason='samples/ fixtures not present (untracked sample data, not in version control)',
)
def test_msna_sample_end_to_end_regression():
    tool_path = SAMPLES / 'MSNA_2023_Questionnaire_Final_CATI_cleaned.xlsx'
    data_path = SAMPLES / 'UKR2308_MSNA_clean_data_230829_full.xlsx'

    label_col, tool_choices, tool_survey = _load_tool(tool_path)
    assert label_col == 'label::English'

    sheets_dat, _ = get_sheets_small_data(data_path)
    data = pd.read_excel(data_path, sheet_name=sheets_dat)
    for sheet_name in sheets_dat:
        # Same pandas 3.0.x StringDtype gap as _pandas2_compat() above, but on
        # the response data itself: disaggregation_creator writes exploded
        # select_multiple answers (Python lists) back into these columns,
        # which StringDtype rejects. Under the pinned pandas 2.0.3 this does
        # convert real int/float columns to object dtype, but harmlessly so
        # for the select_one/select_multiple columns these two tests use.
        data[sheet_name] = data[sheet_name].astype(object)
        data[sheet_name]['overall'] = ' Overall'
        data[sheet_name]['Overall'] = ' Overall'

    daf_final = _build_daf_final([
        {'ID': 1, 'variable': 'A_2_respondent_sex', 'variable_label': 'Sex', 'calculation': None,
         'func': 'select_one', 'admin': 'Overall', 'disaggregations': None, 'disaggregations_label': None,
         'join': None, 'datasheet': 'main'},
        {'ID': 2, 'variable': 'D_2_conflict_damages', 'variable_label': 'Conflict damages', 'calculation': None,
         'func': 'select_multiple', 'admin': 'Overall', 'disaggregations': None, 'disaggregations_label': None,
         'join': None, 'datasheet': 'main'},
    ], tool_survey)

    check_daf_consistency(daf_final, data, sheets_dat, resolve=False)

    result = disaggregation_creator(
        daf_final, data, {}, tool_choices, tool_survey,
        label_colname=label_col, check_significance=False, sm_delimiter=' ',
    )

    assert len(result) == 2
    sm_table = [tbl for tbl, id_, *_ in result if id_ == 2][0]
    decoded_options = set(sm_table['option_orig'].dropna())
    # confirms space-delimited combined answers were actually split into
    # individual, recognizable choice codes - not left as one combined string.
    assert {'damage_to_walls', 'damage_to_floors', 'no_damage_or_noticeable_issue',
            'minor_damage_to_roof_cracks_openings', 'lack_of_space_inside_shelter'}.issubset(decoded_options)
    assert not any(' ' in str(opt) for opt in decoded_options)


@pytest.mark.skipif(
    not (SAMPLES2 / 'library_audit_kobo_form_current.xlsx').exists()
    or not (SAMPLES2 / 'Test_frame.xlsx').exists(),
    reason='samples2/ fixtures not present (untracked sample data, not in version control)',
)
def test_library_audit_sample_new_capability():
    tool_path = SAMPLES2 / 'library_audit_kobo_form_current.xlsx'
    data_path = SAMPLES2 / 'Test_frame.xlsx'

    label_col, tool_choices, tool_survey = _load_tool(tool_path)
    assert label_col == 'label::Ukrainian (uk)'

    sheets_dat, _ = get_sheets_small_data(data_path)
    data = pd.read_excel(data_path, sheet_name=sheets_dat)
    for sheet_name in sheets_dat:
        # Same pandas 3.0.x StringDtype gap as _pandas2_compat() above, but on
        # the response data itself: disaggregation_creator writes exploded
        # select_multiple answers (Python lists) back into these columns,
        # which StringDtype rejects. Under the pinned pandas 2.0.3 this does
        # convert real int/float columns to object dtype, but harmlessly so
        # for the select_one/select_multiple columns these two tests use.
        data[sheet_name] = data[sheet_name].astype(object)
        data[sheet_name]['overall'] = ' Overall'
        data[sheet_name]['Overall'] = ' Overall'

    daf_final = _build_daf_final([
        {'ID': 1, 'variable': 'q1_1', 'variable_label': 'Role', 'calculation': None,
         'func': 'select_one', 'admin': 'Overall', 'disaggregations': None, 'disaggregations_label': None,
         'join': None, 'datasheet': 'libraries'},
        {'ID': 2, 'variable': 'q3_1', 'variable_label': 'Furniture', 'calculation': None,
         'func': 'select_multiple', 'admin': 'Overall', 'disaggregations': None, 'disaggregations_label': None,
         'join': None, 'datasheet': 'libraries'},
    ], tool_survey)

    check_daf_consistency(daf_final, data, sheets_dat, resolve=False)

    result = disaggregation_creator(
        daf_final, data, {}, tool_choices, tool_survey,
        label_colname=label_col, check_significance=False, sm_delimiter='|',
    )

    assert len(result) == 2
    furniture_table = [tbl for tbl, id_, *_ in result if id_ == 2][0]
    decoded_options = set(furniture_table['option_orig'].dropna())
    # confirms the pipe-delimited combinations were actually split into
    # individual furniture choice codes, not left as combined strings.
    assert {'tables', 'chairs', 'free_shelves', 'storage_shelves', 'librarian_desk'}.issubset(decoded_options)


def test_build_group_map_flattens_nested_groups_against_real_tool():
    tool_path = SAMPLES2 / 'library_audit_kobo_form_current.xlsx'
    if not tool_path.exists():
        pytest.skip('samples2/ fixtures not present (untracked sample data, not in version control)')

    label_col, _, _ = _load_tool(tool_path)
    tool_s_raw = pd.read_excel(tool_path, sheet_name='survey')

    group_map = build_group_map(tool_s_raw, label_colname=label_col)

    assert 'gr7' in group_map
    assert {'q7_1', 'q7_2', 'q7_5'}.issubset(set(group_map['gr7']['variables']))
    nested_vars = {'q7_3_reading', 'q7_3_internet', 'q7_3_docs', 'q7_3_teamwork',
                   'q7_3_homework', 'q7_3_other_activity', 'q7_4_primary', 'q7_4_basic',
                   'q7_4_senior', 'q7_online_primary', 'q7_online_basic', 'q7_online_senior'}
    assert nested_vars.issubset(set(group_map['gr7']['variables']))
    # the nested groups are also independently selectable, each with just its own content
    assert set(group_map['gr7_3']['variables']) == {'q7_3_reading', 'q7_3_internet', 'q7_3_docs',
                                                       'q7_3_teamwork', 'q7_3_homework', 'q7_3_other_activity'}


def test_build_group_map_excludes_repeat_content_against_real_tool():
    tool_path = SAMPLES / 'MSNA_2023_Questionnaire_Final_CATI_cleaned.xlsx'
    if not tool_path.exists():
        pytest.skip('samples/ fixtures not present (untracked sample data, not in version control)')

    label_col, _, tool_survey = _load_tool(tool_path)
    tool_s_raw = pd.read_excel(tool_path, sheet_name='survey')

    group_map = build_group_map(tool_s_raw, label_colname=label_col)

    all_grouped_vars = set()
    for info in group_map.values():
        all_grouped_vars.update(info['variables'])

    # hh_members / healthcare are real repeat-group sheets in this tool
    # (68 + 8 eligible questions respectively, confirmed directly against
    # the tool) - none of their questions should ever appear in any
    # group's variable list.
    repeat_vars = set(tool_survey.loc[tool_survey['datasheet'] != 'main', 'name'])
    assert len(repeat_vars) > 0  # sanity check the fixture actually has repeat content
    assert repeat_vars.isdisjoint(all_grouped_vars)


def test_generate_daf_rows_against_real_tool_survey():
    tool_path = SAMPLES2 / 'library_audit_kobo_form_current.xlsx'
    if not tool_path.exists():
        pytest.skip('samples2/ fixtures not present (untracked sample data, not in version control)')

    label_col, _, tool_survey = _load_tool(tool_path)

    result = generate_daf_rows(
        dependent_vars=['q1_1', 'q3_1'],
        admins=['oblast'],
        disaggregations=['q9_5'],
        include_overall_admin=True,
        tool_survey=tool_survey,
        label_colname=label_col,
    )

    # 2 variables * 2 admins (Overall + oblast) * (1 + 1 disaggregation) = 8 rows
    assert len(result) == 8
    func_by_var = dict(zip(result['variable'], result['func']))
    assert func_by_var['q1_1'] == 'select_one'
    assert func_by_var['q3_1'] == 'select_multiple'
    expected_labels = {
        tool_survey.loc[tool_survey['name'] == 'q1_1', label_col].iloc[0],
        tool_survey.loc[tool_survey['name'] == 'q3_1', label_col].iloc[0],
    }
    assert expected_labels.issubset(set(result['variable_label']))
