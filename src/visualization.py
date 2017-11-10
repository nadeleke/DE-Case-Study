import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def main():

    with open('../Results/table_names.txt', 'r') as f:
        table_names = f.readlines()
    f.close()

    # removing \n character from str
    table_names = [table_name.rstrip() for table_name in table_names]

    # Initialization
    check = 0
    t_names = []

    skip_list = ['airport', 'carrier_history', 'system_fields', 'yearly_data', #'biannual_data_1', 'biannual_data_2',
                 'quarterly_data_1', 'quarterly_data_2', 'quarterly_data_3', 'quarterly_data_4']
    for table_name in table_names:
        if table_name in skip_list:
            continue
        else:
            # current list of table names in analysis
            t_names.append(table_name)

            prefix = 'WN_'
            # prefix = 'DL_'
            # prefix = 'EV_'
            # prefix = 'lim_'
            # prefix = 'all_'

            with open('../Results/{0}cat_analytics_{1}.csv'.format(prefix, table_name), 'r') as f:
                cat_df = pd.read_csv(f, index_col=0)
            f.close()

            with open('../Results/{0}num_analytics_{1}.csv'.format(prefix, table_name), 'r') as f:
                num_df = pd.read_csv(f, index_col=0)
            f.close()

            # print(cat_df)
            cat_count_df = cat_df.loc['count', :]
            unique_df = cat_df.loc['unique', :]
            top_df = cat_df.loc['top', :]
            freq_df = cat_df.loc['freq', :]

            # print(num_df)
            num_count_df = num_df.loc['count', :]
            mean_df = num_df.loc['mean', :]
            std_df = num_df.loc['std', :]
            min_df = num_df.loc['min', :]
            max_df = num_df.loc['max', :]

            if check == 1:
                all_cat_count_df = pd.concat((all_cat_count_df, cat_count_df), axis=1)
                all_unique_df = pd.concat((all_unique_df, unique_df), axis=1)
                all_top_df = pd.concat((all_top_df, top_df), axis=1)
                all_freq_df = pd.concat((all_freq_df, freq_df), axis=1)
                all_num_count_df = pd.concat((all_num_count_df, num_count_df), axis=1)
                all_mean_df = pd.concat((all_mean_df, mean_df), axis=1)
                all_std_df = pd.concat((all_std_df, std_df), axis=1)
                all_min_df = pd.concat((all_min_df, min_df), axis=1)
                all_max_df = pd.concat((all_max_df, max_df), axis=1)
            else:
                check = 1
                all_cat_count_df = cat_count_df
                all_unique_df = unique_df
                all_top_df = top_df
                all_freq_df = freq_df
                all_num_count_df = num_count_df
                all_mean_df = mean_df
                all_std_df = std_df
                all_min_df = min_df
                all_max_df = max_df

    all_cat_count_df.columns = t_names
    all_unique_df.columns = t_names
    all_top_df.columns = t_names
    all_freq_df.columns = t_names
    all_num_count_df.columns = t_names
    all_mean_df.columns = t_names
    all_std_df.columns = t_names
    all_min_df.columns = t_names
    all_max_df.columns = t_names

    # print(all_cat_count_df)
    # print(all_unique_df)
    # print(all_top_df)  # <-- focus here
    # print(all_freq_df)  # <-- focus here
    # print(all_num_count_df)
    # print(all_mean_df)  # <-- focus here
    # print(all_std_df)  # <-- focus here
    # print(all_min_df)
    # print(all_max_df)

    # ------------------------------------------------
    # plot results
    # ------------------------------------------------
    print(all_mean_df)
    all_mean_df = all_mean_df.T
    all_mean_df.reset_index(inplace=True)
    all_mean_df.drop('index', axis=1, inplace=True)
    print(all_mean_df)
    print(all_mean_df.index)
    col_groups = [['dep_del15', 'arr_del15'], ['cancelled', 'diverted'], ['dep_delay_group', 'arr_delay_group']]

    # determining image title (need to adjust title for new where clause in analysis)
    carriers = ' carriers' if prefix in ['all_', 'lim_'] else ''
    prefix = 'top 3 ' if prefix == 'lim_' else prefix
    for col_to_plot in col_groups:
        # carriers = ''
        if col_to_plot == ['dep_del15', 'arr_del15']:
            title = 'Ave. delay for {0}{1}'.format(prefix[:-1], carriers)
        elif col_to_plot == ['cancelled', 'diverted']:
            title = 'Ave. cancellation and plane diversion for {0}{1}'.format(prefix[:-1], carriers)
        elif col_to_plot == ['dep_delay_group', 'arr_delay_group']:
            title = 'Ave. delay interval (1 every 15 minutes) for {0}{1}'.format(prefix[:-1], carriers)

        plot = all_mean_df.plot(x=all_mean_df.index+1, y=col_to_plot, kind="bar", title=title)
        plot.set_xlabel("Month Number (NOTE: 13 & 14 = 1st and 2nd 6 months of the year)")
        plot.set_ylabel("fraction")
        f = plot.get_figure()
        suffix = ''.join(col_to_plot)
        f.savefig('../Results/plots/{}_mean_{}_{}.png'.format(prefix, table_name, suffix), bbox_inches='tight')
        plt.show()


    # y = np.random.rand(10, 4)
    # y[:, 0] = np.arange(10)
    # df = pd.DataFrame(y, columns=["X", "A", "B", "C"])
    # df['index1'] = df.index
    # print(df)
    # ax = df.plot(x="index1", y="A", kind="bar")
    # df.plot(x="X", y="B", kind="bar", ax=ax, color="C2")
    # df.plot(x="X", y="C", kind="bar", ax=ax, color="C3")
    # plt.show()



    # df.set_index('index', inplace=True)
    # df.transpose()
    # print(df)
    # ax = df.plot(x="X", y="A", kind="bar")
    # df.plot(x="X", y="B", kind="bar", ax=ax, color="C2")
    # df.plot(x="X", y="C", kind="bar", ax=ax, color="C3")
    # plt.show()



if __name__ == '__main__':
    main()