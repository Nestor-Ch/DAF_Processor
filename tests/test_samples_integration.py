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

SAMPLES = 'samples'
SAMPLES2 = 'samples2'


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


def test_msna_sample_end_to_end_regression():
    tool_path = f'{SAMPLES}/MSNA_2023_Questionnaire_Final_CATI_cleaned.xlsx'
    data_path = f'{SAMPLES}/UKR2308_MSNA_clean_data_230829_full.xlsx'

    label_col, tool_choices, tool_survey = _load_tool(tool_path)
    assert label_col == 'label::English'

    sheets_dat, _ = get_sheets_small_data(data_path)
    data = pd.read_excel(data_path, sheet_name=sheets_dat)
    for sheet_name in sheets_dat:
        # Same pandas 3.0.x StringDtype gap as _pandas2_compat() above, but on
        # the response data itself: disaggregation_creator writes exploded
        # select_multiple answers (Python lists) back into these columns,
        # which StringDtype rejects. A no-op under the pinned pandas 2.0.3.
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
    # space-delimited MSNA select_multiple answers must still explode into
    # individual, recognizable choice labels - not one combined string.
    assert sm_table['option'].nunique() > 1
    assert not any('  ' in str(opt) for opt in sm_table['option'].dropna())


def test_library_audit_sample_new_capability():
    tool_path = f'{SAMPLES2}/library_audit_kobo_form_current.xlsx'
    data_path = f'{SAMPLES2}/Test_frame.xlsx'

    label_col, tool_choices, tool_survey = _load_tool(tool_path)
    assert label_col == 'label::Ukrainian (uk)'

    sheets_dat, _ = get_sheets_small_data(data_path)
    data = pd.read_excel(data_path, sheet_name=sheets_dat)
    for sheet_name in sheets_dat:
        # Same pandas 3.0.x StringDtype gap as _pandas2_compat() above, but on
        # the response data itself: disaggregation_creator writes exploded
        # select_multiple answers (Python lists) back into these columns,
        # which StringDtype rejects. A no-op under the pinned pandas 2.0.3.
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
