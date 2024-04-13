import pandas as pd
from scipy.stats import zscore

column_names = [
    'Time', 'Duration', 'SrcDevice', 'DstDevice', 'Protocol',
    'SrcPort', 'DstPort', 'SrcPackets', 'DstPackets', 'SrcBytes', 'DstBytes'
]

dataset = pd.read_csv('netflow_day-14.csv', header=None, names=column_names)

print(dataset.head())

columns_to_analyze = ['Duration', 'SrcPackets', 'DstPackets', 'SrcBytes', 'DstBytes']
outliers_all = pd.DataFrame()


for column in columns_to_analyze:
    dataset[f'Z-Score_{column}'] = zscore(dataset[column])

    outliers = dataset[(dataset[f'Z-Score_{column}'] > 30) | (dataset[f'Z-Score_{column}'] < -30)]

    if not outliers.empty:
        outliers['Analyzed_Column'] = column
        outliers_all = pd.concat([outliers_all, outliers], ignore_index=True)

        # Print outliers if any
        print(f"Outliers based on Z-score analysis for {column}:")
        print(outliers)

# dataset.to_excel('complete_dataset_with_zscores.xlsx', index=False)
outliers_all.to_excel('outliers_detected.xlsx', index=False)