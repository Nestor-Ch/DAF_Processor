import re

import pandas as pd


ELIGIBLE_DEPENDENT_TYPES = {'select_one', 'select_multiple', 'integer', 'decimal'}


def build_group_map(tool_survey_raw, label_colname=None):

    """
    Walk the raw 'survey' sheet of a Kobo tool and map each begin_group/
    end_group block to the eligible dependent-variable questions it
    (recursively) contains.

    Parameters:
    ----------
    tool_survey_raw : pd.DataFrame
        The UNFILTERED 'survey' sheet, as read directly via
        pd.read_excel(tool_path, sheet_name='survey') - not the output of
        load_tool_survey(), which already drops group markers and rows
        outside the eligible q.types before this function would ever see
        them. Must have at least 'type' and 'name' columns.

    label_colname : str, optional
        Column to pull each group's own display label from (the group's
        own row, e.g. 'label::English'). If None, or the column doesn't
        exist / is blank on a given group's row, that group's raw `name`
        is used as its label instead.

    Returns:
    -------
    dict[str, dict]
        Keyed by group name. Each value is
        {'label': str, 'variables': list[str]}
        where 'variables' lists eligible (select_one / select_multiple /
        integer / decimal) question names belonging to that group or any
        of its nested sub-groups, in tool order, deduplicated. Groups
        with zero eligible variables (after excluding repeat content) are
        omitted from the result entirely.
    """

    group_stack = []
    group_info = {}
    in_repeat_depth = 0

    for _, row in tool_survey_raw.iterrows():
        row_type = row['type']
        if not isinstance(row_type, str):
            continue
        row_name = row['name']

        if re.search(r'begin[_ ]repeat', row_type):
            in_repeat_depth += 1
            continue
        if re.search(r'end[_ ]repeat', row_type):
            in_repeat_depth = max(0, in_repeat_depth - 1)
            continue
        if in_repeat_depth > 0:
            continue

        if re.search(r'begin[_ ]group', row_type, re.IGNORECASE):
            label = row_name
            if label_colname is not None and label_colname in tool_survey_raw.columns:
                candidate = row.get(label_colname)
                if isinstance(candidate, str) and candidate.strip():
                    label = candidate
            group_stack.append(row_name)
            group_info[row_name] = {'label': label, 'variables': []}
            continue

        if re.search(r'end[_ ]group', row_type, re.IGNORECASE):
            if group_stack:
                group_stack.pop()
            continue

        q_type = re.split(r'\s', row_type)[0]
        if q_type in ELIGIBLE_DEPENDENT_TYPES:
            for group_name in group_stack:
                if row_name not in group_info[group_name]['variables']:
                    group_info[group_name]['variables'].append(row_name)

    return {name: info for name, info in group_info.items() if info['variables']}


def generate_daf_rows(dependent_vars, admins, disaggregations, include_overall_admin,
                       tool_survey, label_colname):

    """
    Build DAF 'main'-sheet rows for a set of dependent variables against a
    set of admin and disaggregation columns.

    Combination rule (per dependent variable V, "paired"): one row for
    each admin in the effective admin list (blank disaggregation), plus
    one row for every (admin, disaggregation) pair. No row combines
    multiple disaggregations together.

    Parameters
    ----------
    dependent_vars : list[str]
        Kobo question names to disaggregate. Must all be present in
        tool_survey['name'] - this function does not validate that, it's
        the caller's responsibility.

    admins : list[str]
        Admin column names as picked or typed by the user. May be real
        Kobo question names, arbitrary custom text, or the literal
        string 'Overall'.

    disaggregations : list[str]
        Same rules as `admins`. May be empty.

    include_overall_admin : bool
        If True, ensures the literal string 'Overall' is present in the
        effective admin list (prepended, if not already given in
        `admins`).

    tool_survey : pd.DataFrame
        The tool's processed survey sheet (load_tool_survey's output),
        used only to look up each Kobo-sourced name's q.type (for `func`)
        and label text (for `variable_label`/`disaggregations_label`). A
        name not found here falls back to using itself as its own label.

    label_colname : str
        The resolved label column name used for the tool_survey lookup
        above.

    Returns
    -------
    pd.DataFrame
        Columns, in order: ID, variable, variable_label, calculation,
        func, admin, disaggregations, disaggregations_label, join -
        exactly the DAF main-sheet schema. `calculation` and `join` are
        always None. `ID` is sequential starting at 1. Blank-disaggregation
        rows get disaggregations_label='Overall'.
    """

    func_map = {'select_one': 'select_one', 'select_multiple': 'select_multiple',
                'integer': 'numeric', 'decimal': 'numeric'}

    def _label_for(name):
        match = tool_survey.loc[tool_survey['name'] == name, label_colname]
        if len(match) > 0 and isinstance(match.iloc[0], str) and match.iloc[0].strip():
            return match.iloc[0]
        return name

    def _func_for(name):
        match = tool_survey.loc[tool_survey['name'] == name, 'q.type']
        if len(match) > 0:
            return func_map.get(match.iloc[0], 'select_one')
        return 'select_one'

    effective_admins = list(admins)
    if include_overall_admin and 'Overall' not in effective_admins:
        effective_admins = ['Overall'] + effective_admins

    rows = []
    row_id = 1
    for var in dependent_vars:
        var_label = _label_for(var)
        func = _func_for(var)
        for admin in effective_admins:
            if admin == var:
                continue
            rows.append({
                'ID': row_id, 'variable': var, 'variable_label': var_label,
                'calculation': None, 'func': func, 'admin': admin,
                'disaggregations': None, 'disaggregations_label': 'Overall', 'join': None,
            })
            row_id += 1
            for disagg in disaggregations:
                if disagg == var or disagg == admin:
                    continue
                rows.append({
                    'ID': row_id, 'variable': var, 'variable_label': var_label,
                    'calculation': None, 'func': func, 'admin': admin,
                    'disaggregations': disagg, 'disaggregations_label': _label_for(disagg), 'join': None,
                })
                row_id += 1

    columns = ['ID', 'variable', 'variable_label', 'calculation', 'func',
               'admin', 'disaggregations', 'disaggregations_label', 'join']
    return pd.DataFrame(rows, columns=columns)
