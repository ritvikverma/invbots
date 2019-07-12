import pandas as pd

input_path = 'input_item_value_table/'
file_name = 'i_sample'
df = pd.read_csv('i_sample.csv')
df['value_content'].fillna(0, inplace=True)
# columns to record the invbots_key lableled
df.loc[:, 'as_parent'] = 0
df.loc[:, 'as_child'] = 0

calculation_header = ['table_id', 'from_value_row_index', 'from_value_column_index', 'to_value_row_index', 'to_value_column_index', 'weight']
# to hard code the table_id for development
table_id = 1
calculation_df = pd.DataFrame(columns=calculation_header)
calculation_list = []

for i in range(len(df)):
    name_total = df.loc[(df['value_row_index'] == i, 'item_name')]
    # try: change the data type of "value_content" from string to float
    # except: deal with "value_content" = 0
    try:
        value_total = float(df.loc[(df['value_row_index'] == i, 'value_content')].values[0].replace(',',''))
    except:
        value_total = float(df.loc[(df['value_row_index'] == i, 'value_content')].values[0])
    # find the total for non zero "value_content" only
    if value_total != 0:
        # j starts with (i - 1) and it is a iterator of reverse (i - 1)
        j = i - 1
        sum_sub_total = 0
        # to store the value_row_index of sub_total components
        index_sub_total = []
        while (j != 0) and (value_total != sum_sub_total):
            # try: change the data type of "value_content" from string to float
            # except: deal with "value_content" = 0
            try:
                sum_sub_total += float(df.loc[(df['value_row_index'] == j, 'value_content')].values[0].replace(',',''))
            except:
                sum_sub_total += float(df.loc[(df['value_row_index'] == j, 'value_content')].values[0])
            name_sub_total = df.loc[(df['value_row_index'] == j, 'item_name')]
            # collect the index of components
            index_sub_total.append(j)
            j -= 1
        if value_total == sum_sub_total:
            # in item_value_table df, record the invbot_key labelled
            df.loc[i, 'as_parent'] = i
            df.loc[df['value_row_index'].isin(index_sub_total), 'as_child'] = i
            # append records in "calculation_table"
            for k in index_sub_total:
                calculation_list.append([table_id, df.loc[i, 'value_row_index'], df.loc[i, 'value_column_index'], df.loc[k, 'value_row_index'], df.loc[k, 'value_column_index'], 1])
calculation_df = calculation_df.append(pd.DataFrame(calculation_list, columns=calculation_header), ignore_index=True)
print(calculation_df)