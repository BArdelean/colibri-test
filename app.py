import pandas as pd
import glob

class WindTurbineData:
    def __init__(self, file):
        self.file = file
        self.data = self.read_data()

    def read_data(self):
        data_frames = []
        for filename in glob.glob(self.file):
            try:
                df = pd.read_csv(filename, parse_dates=['timestamp'])
                data_frames.append(df)
            except Exception as e:
                print(f"Error reading {filename}: {e}")
        return pd.concat(data_frames)

    def clean_data(self):
        self.data['power_output'] = pd.to_numeric(self.data['power_output'], errors='coerce')
        self.data['power_output'] = self.data['power_output'].fillna(method='ffill')  # fill missing values with the last valid value
        self.data.to_csv('output_data/cleaned_data.csv', index=False)
        print("Cleaned_data written to file: output_data/cleaned_data.csv")


    def calculate_summary_statistics(self):
        summary_stats = self.data.groupby('turbine_id').agg(
            min_power_output=('power_output', 'min'),
            max_power_output=('power_output', 'max'),
            avg_power_output=('power_output', 'mean')
        )
        summary_stats.to_csv('output_data/summary_statistics.csv')
        print("Summary statistics written to file: output_data/summary_statistics.csv")

    def identify_anomalies(self):
        anomalies = []
        grouped_data = self.data.groupby('turbine_id')
        for turbine_id, group in grouped_data:
            mean_output = group['power_output'].mean()
            std_dev_output = group['power_output'].std()
            lower_bound = mean_output - 2 * std_dev_output
            upper_bound = mean_output + 2 * std_dev_output
            anomalies_df = group[(group['power_output'] < lower_bound) | (group['power_output'] > upper_bound)]
            anomalies.append(anomalies_df)
        anomalies_data = pd.concat(anomalies)
        anomalies_data.to_csv('output_data/anomalies.csv', index=False)
        print("Anomalies written to file: output_data/anomalies.csv")

if __name__ == "__main__":
    pipeline = WindTurbineData("input_data/data_group_*.csv")
    pipeline.clean_data()
    pipeline.calculate_summary_statistics()
    pipeline.identify_anomalies()
