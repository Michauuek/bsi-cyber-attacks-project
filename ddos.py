import pandas as pd
from sklearn.ensemble import IsolationForest

column_names = [
    'Time', 'Duration', 'SrcDevice', 'DstDevice', 'Protocol',
    'SrcPort', 'DstPort', 'SrcPackets', 'DstPackets', 'SrcBytes', 'DstBytes'
]

dataset = pd.read_csv('netflow_day-14.csv', header=None, names=column_names)

# Convert 'Time' from epoch to datetime
# and round it to the nearest minute
dataset['Time'] = (dataset['Time'] // 60) * 60
dataset['Time'] = pd.to_datetime(dataset['Time'], unit='s')

aggregated_data = dataset.groupby(['SrcDevice', 'Time']).agg({
    'SrcPackets': 'sum',
    'SrcBytes': 'sum',
    'DstDevice': pd.Series.nunique  # Count unique destination devices
}).reset_index()

print(aggregated_data.head())

# Anomaly Detection Using Isolation Forest to detect outliers
model = IsolationForest(n_estimators=100, contamination=0.01)
aggregated_data['anomaly'] = model.fit_predict(aggregated_data[['SrcPackets', 'SrcBytes']])

# Print potential attacks
# -1 is considered as anomaly
potential_attacks = aggregated_data[aggregated_data['anomaly'] == -1]
potential_attacks.to_excel('potential_attacks_ddos.xlsx', index=False)

sorted_data = aggregated_data.sort_values(by=['DstDevice', 'SrcPackets', 'SrcBytes'], ascending=[False, False, False])
sorted_data.to_excel('ddos_to_analyse.xlsx', index=False)