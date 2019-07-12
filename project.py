import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None  # default='warn'
input_path = 'item_value_table_input_sheet_20190704_on_20190710'
table_table_location_path = 'table_table_input_sheet_20190704_on_20190710'
calculation_header = ['table_id', 'from_value_row_index', 'from_value_column_index', 'to_value_row_index',
                      'to_value_column_index', 'weight']
error_header = ['table_name', 'table_id', 'column_id', 'error_type']

error = pd.DataFrame(columns=error_header)

errors = []


def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)


def str_to_float(s):
    return (float(s))


def to_str(s):
    return (str(s))


def bracket_to_negative(s):
    s = str(s)
    if (hasNumbers(s)):
        if '(' in s:
            s = s.replace('(', '')
            s = s.replace(')', '')
            s = '-' + s
    else:
        s = '0'
    return s


def dollar_remove(s):
    s.replace('HK$', '')
    return s


def clean_bs(df):
    df['value_content'] = df['value_content'].apply(to_str)
    df['value_content'] = df['value_content'].str.strip()
    df['value_content'] = df['value_content'].str.replace('\t', '')
    df['value_content'] = df['value_content'].str.replace(',', '')
    df['value_content'] = df['value_content'].str.replace('HK\$', '')
    df['value_content'] = df['value_content'].str.replace('\’', '')
    # Billy: I am thinking if we should fillna
    df['value_content'] = df['value_content'].fillna(0)
    df['value_content'] = df['value_content'].apply(bracket_to_negative)
    df['value_content'] = df['value_content'].apply(str_to_float)
    return df


def create_calculation_table(child_row_index_list, table_id, parent_row_index, parent_col_index, weight):
    for k in child_row_index_list:
        calculation_list.append([table_id, parent_row_index, parent_col_index, k, parent_col_index, weight])


def check_is_consecutive(l):
    maximum = max(l)
    if sum(l) == maximum * (maximum + 1) / 2:
        return True
    return False


def operate_sum_bs(df, col_index, table_id):
    # to hard code the table_id for development
    for index, row in df.iterrows():  # iterate through the rows of the temp df
        value_total = row['value_content']  # value_content of the row for knowing the total
        parent_row_index = row['value_row_index']  # current parent's value_row_index
        parent_col_index = col_index  # current parent's value_column_index
        # to skip the "value_content" = 0,
        if value_total != 0:  # we don't need those that have the total as 0
            j = parent_row_index - 1  # index of the first child, subtract by 1 to find if we have a case of non-consecutive
            sum_sub_total = 0  # to tally the total till now
            child_row_index_list = []  # this finds rows of the children
            # j != -1 -> to prevent the error when "value_column_index" = 0
            while j != -1 and value_total != sum_sub_total:  # till children reach the first element of the df
                # name_sub_total = row['item_name']
                if (whole_df.loc[((whole_df['value_row_index'] == j) & (
                        whole_df['value_column_index'] == parent_col_index)), 'as_child'] == 0).values[
                    0]:  # if, in the whole_df, the child's value is 0
                    sum_sub_total += df.loc[df['value_row_index'] == j, 'value_content'].values[
                        0]  # adds the child value_index to the current total
                    child_row_index_list.append(j)  # adds the position of the child to the list
                j -= 1  # decrement to find one more child
            if value_total == sum_sub_total:  # if we have a match
                parent_label_condition = ((whole_df['value_row_index'] == parent_row_index) & (
                            whole_df['value_column_index'] == parent_col_index))
                whole_df.loc[
                    parent_label_condition, 'as_parent'] = parent_row_index  # assigns the values of the parent to the parent
                for child_row_index in child_row_index_list:
                    child_label_condition = ((whole_df['value_row_index'] == child_row_index) & (
                                whole_df['value_column_index'] == parent_col_index))
                    whole_df.loc[
                        child_label_condition, 'as_child'] = parent_row_index  # assigns the values of the child to the child
                create_calculation_table(child_row_index_list, table_id, parent_row_index, parent_col_index, 1)


def operate_minus_bs(df, col_index, table_id):  # A — B = C
    df = df.reset_index()
    df_parents = df.loc[df['as_child'] == 0].loc[df['as_parent'] == 0].loc[
        df['value_content'] != 0]  # sets all the eligible parents for subtraction (C)
    for index_parent, parent in df_parents.iterrows():
        b_index = parent['value_row_index'] - 1  # selects first child (-B)
        found = False
        while found == False and b_index > 0:
            if (df.loc[b_index, 'value_content'] == 0):
                b_index -= 1
                continue
            else:
                a_index = b_index - 1  # selects first sub-child for (A)
                while (found == False and a_index >= 0):
                    if (df.loc[a_index, 'value_content'] == 0):
                        a_index -= 1
                        continue
                    else:
                        a = df.loc[a_index, 'value_content']
                        b = df.loc[b_index, 'value_content']
                        p = parent['value_content']
                        if (a - b == p):
                            found = True
                            parent['as_parent'] = parent['value_row_index']  # sets parents and children
                            df.loc[a_index, 'as_child'] = parent['value_row_index']
                            df.loc[b_index, 'as_child'] = parent['value_row_index']
                            calculation_list.append(
                                [table_id, parent['value_row_index'],
                                 col_index,
                                 df.loc[a_index, 'value_row_index'], col_index, 1])
                            calculation_list.append(
                                [table_id, parent['value_row_index'],
                                 col_index,
                                 df.loc[b_index, 'value_row_index'], col_index, -1])
                        a_index -= 1
                b_index -= 1
    return df


if __name__ == "__main__":
    input_path += '.csv'
    table_table_location_path += '.csv'
    table_df = pd.read_csv(table_table_location_path)
    main_df = pd.read_csv(input_path)
    table_names = ['bs']
    truth_false = []
    for table_name in table_names:
        # get the "table_id" that equal to "table_name"
        l = list(table_df.loc[table_df['table_name'] == table_name]['table_id'])
        for table_id in l:
            print(table_name, table_id)
            calculation_df = pd.DataFrame(columns=calculation_header)
            output_location = '~/Desktop/'
            output_path = str(table_id)
            output_path += '_'
            output_path += table_name
            output_path += '.csv'
            output_location += output_path
            # select a separated table based on "table_id"
            whole_df = main_df.loc[main_df['table_id'] == table_id]
            whole_df.loc[:, 'as_parent'] = 0
            whole_df.loc[:, 'as_child'] = 0
            # create calculation list
            calculation_list = []
            # get a list of "value_column_index" from whole_df
            value_col_index_list = list(set(whole_df['value_column_index']))
            value_col_index_list = list(dict.fromkeys(value_col_index_list))
            final_df = whole_df
            final_df = final_df.iloc[0:0]
            check_val = whole_df['value_content']
            for value_col_index in value_col_index_list:
                l = []
                temp_df = whole_df.loc[whole_df['value_column_index'] == value_col_index, :]
                temp_df.sort_values(by=['value_row_index'], inplace=True)
                for index, row in temp_df.iterrows():
                    l.append(row['value_row_index'])
                new_list = [x + 1 for x in l]
                if (len(new_list) != 0):
                    if (check_is_consecutive(new_list) == False):
                        errors.append([table_name, table_id, value_col_index, 'Non consecutive'])

            # to process each column one by one
            for value_col_index in value_col_index_list:
                print('val_col_index =', value_col_index)
                # select the table subset by "value_column_index"
                temp_df = whole_df.loc[whole_df['value_column_index'] == value_col_index, :]
                # sort the values by "value_row_index"
                temp_df.sort_values(by=['value_row_index'], inplace=True)
                temp_df = clean_bs(temp_df)
                operate_sum_bs(temp_df, value_col_index, table_id)
                calculation_df = calculation_df.append(pd.DataFrame(calculation_list, columns=calculation_header),
                                                       ignore_index=True)
            print("Enters sum")
            for value_col_index in value_col_index_list:
                temp_df = whole_df.loc[whole_df['value_column_index'] == value_col_index, :]
                temp_df.sort_values(by=['value_row_index'], inplace=True)
                temp_df = clean_bs(temp_df)
                temp_df = operate_minus_bs(temp_df, value_col_index, table_id)
                final_df = final_df.append(temp_df, sort=False)
                calculation_df = calculation_df.append(pd.DataFrame(calculation_list, columns=calculation_header),
                                                       ignore_index=True)
            print("Enters minus")
            final_df.to_csv(output_location)
            calculation_df.to_csv('~/Desktop/' + output_path.replace('.csv', '_') + 'calculation_df.csv')
    error = error.append(pd.DataFrame(errors, columns=error_header), ignore_index=True)