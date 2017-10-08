import pandas as pd
import os
from threading import Thread as Process


# Verify OGD data path
# print os.listdir("OGD/")

def spaces():
    print "\n\n\n\n\n"


# Store states in the format of slug:State
indian_states = {
    'Jammu': 'Jammu & Kashmir',
    'Himachal': 'Himachal Pradesh',
    'Uttarakhand': 'Uttarakhand',
    'Punjab': 'Punjab',
    'Jharkhand': 'Jharkhand',
    'Haryana': 'Haryana',
    'Odisha': 'Odisha',
    'Uttar': 'Uttar Pradesh',
    'Assam': 'Assam',
    'Bihar': 'Bihar',
    'Madhya': 'Madhya Pradesh',
    'Rajasthan': 'Rajasthan',
    'Chhattisgarh': 'Chhattisgarh',
    'Nagaland': 'Nagaland',
    'Bengal': 'West Bengal',
    'Telangana': 'Telangana',
    'Tripura': 'Tripura',
    'Sikkim': 'Sikkim',
    'Tamil': 'Tamil Nadu',
    'Gujarat': 'Gujarat',
    'Arunachal': 'Arunachal Pradesh',
    'Delhi': 'Delhi',
    'Goa': 'Goa',
    'Daman': 'Daman and Diu',
    'Andaman': 'Andaman and Nicobar Islands',
    'Andhra': 'Andhra Pradesh',
    'Puducherry': 'Puducherry',
    'Dadra': 'Dadra and Nagar Haveli',
    'State': 'State/UT Wise'

}

# Dict with the xcel data loaded as pandas Dataframes
pd_csv = {}


def create_pd_csv_pd(excel, pd_csv):
    """
    Read the excel files as pandas DataFrame
    """
    pd_csv[excel] = pd.read_pickle("src/" + excel)


processes = []
for val in os.listdir("src/"):
    processes.append(Process(target=create_pd_csv_pd, args=(val, pd_csv,)))
for process in processes:
    process.start()
for process in processes:
    process.join()


# print pd_csv.keys()


def compute_data_sizes(pd_csv):
    """
    Isolate the different Units available
    Compute the sum with unit conversion
    store output in MB
    """
    Units = {u'KB': 1024, u'bytes': 1, u'MB': (1024 * 1024)}
    data_sizes = {}
    for name, xcel in pd_csv.iteritems():
        count = 0
        for val in xcel['File Size']:
            try:
                quant = val.split(" ")
            except AttributeError:
                continue
            count += float(quant[0]) * Units[quant[1]]
        data_sizes[name] = (count / (1024 * 1024))
    return data_sizes


print compute_data_sizes(pd_csv)

pd_csv_state_class_resource_title = {}


def derive_state_row(xcel):
    '''
    Classify the Resource Titles to Individual States
    '''
    # pd_csv[xcel].insert(13,'State',[n for n in range(48386)], allow_duplicates=False)
    # pd_csv[xcel].columns

    state_list = pd.Series(range(pd_csv[xcel]['Resource Title'].count()))

    for i in range(pd_csv[xcel]['Resource Title'].count()):

        for key in indian_states.keys():

            if key in pd_csv[xcel]['Resource Title'][i]:
                state_list[i] = indian_states[key]
                break

    pd_csv_state_class_resource_title[xcel] = state_list


processes = []
for val in pd_csv.keys():
    processes.append(Process(target=derive_state_row, args=(val,)))
for process in processes:
    process.start()
for process in processes:
    process.join()


# print pd_csv_state_class_resource_title

def percentage_unclassified_resource_title(pd_csv):
    state_list = pd_csv_state_class_resource_title
    for state in state_list.values():
        state_list_value_counts = state.value_counts()
        count = 0
        for val in state_list_value_counts.index:
            if state_list_value_counts[val] == 1:
                count += 1
        print round(float(count) / 48386 * 100)


percentage_unclassified_resource_title(pd_csv)
