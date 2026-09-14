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
