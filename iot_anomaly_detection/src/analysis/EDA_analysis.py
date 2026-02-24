# eda_analysis.py
from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import numpy as np
from influxdb_client import InfluxDBClient
import os


def get_stream_data(topic='raw-sensor-data', bootstrap_servers='localhost:9092', max_messages=3000):
    """
    Read the latest messages from Kafka topic and convert to DataFrame
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=10000  # timeout after 10 seconds
    )
    
    data = []
    print(f" Fetching up to {max_messages} messages from Kafka topic '{topic}'...")
    
    try:
        for i, msg in enumerate(consumer):
            record = msg.value
            data.append({
                'time': datetime.fromisoformat(record['timestamp'].replace('Z', '+00:00')),
                'sensor_id': record['sensor_id'],
                'sensor_type': record['sensor_type'],
                'location': record.get('location', 'unknown'),
                'value': record['value'],
                'unit': record.get('unit', ''),
                'is_anomaly': record.get('is_anomaly', False)
            })
            if i+1 >= max_messages:
                break
    except Exception as e:
        print(f" Error reading from Kafka: {e}")
    finally:
        consumer.close()
    
    df = pd.DataFrame(data)
    print(f" Retrieved {len(df)} messages from Kafka")
    return df

# -----------------------------
# Save data to CSV function
# -----------------------------
def save_to_csv(df, filename='cleaned_iot_data.csv'):
    """
    Save cleaned data to CSV file
    """
    if len(df) == 0:
        print(" No data to save!")
        return False
    
    # Additional cleaning before saving
    df_clean = df.copy()
    
    # Sort by time
    df_clean = df_clean.sort_values('time')
    
    # Remove duplicates (same sensor_id within the same second)
    df_clean['time_round'] = df_clean['time'].dt.round('1s')
    df_clean = df_clean.drop_duplicates(subset=['sensor_id', 'time_round'], keep='first')
    df_clean = df_clean.drop('time_round', axis=1)
    
    # Reset index
    df_clean = df_clean.reset_index(drop=True)
    
    # Save to CSV
    try:
        df_clean.to_csv(filename, index=False, date_format='%Y-%m-%d %H:%M:%S')
        print(f" Data saved to '{filename}'")
        print(f"   - Records: {len(df_clean)}")
        print(f"   - Columns: {list(df_clean.columns)}")
        print(f"   - Time range: {df_clean['time'].min()} to {df_clean['time'].max()}")
        return True
    except Exception as e:
        print(f" Error saving CSV: {e}")
        return False

# -----------------------------
# Main EDA Class 
# -----------------------------
class IoTDataAnalysis:
    def __init__(self, token=None, org="iot_anomaly_detection", url="http://localhost:8086"):
        if token:
            self.client = InfluxDBClient(url=url, token=token, org=org)
            self.query_api = self.client.query_api()
        else:
            self.client = None
            self.query_api = None
        self.org = org
        
    # -----------------------------
    # Get data
    # -----------------------------
    def get_sensor_data(self, df=None, hours=0, minutes=0):
        """
        Use provided DataFrame directly or fetch from InfluxDB
        """
        if df is not None:
            print(" Using provided DataFrame for EDA...")
            return df
        
        if self.query_api is None:
            raise ValueError("No DataFrame provided and InfluxDB not initialized!")
        
        flux_range = ""
        if hours > 0:
            flux_range += f"{hours}h"
        if minutes > 0:
            flux_range += f"{minutes}m"
        if flux_range == "":
            flux_range = "1h"  # Default
        
        print(f" Fetching last {hours} hour(s) and {minutes} minute(s) from InfluxDB...")
        
        query = f'''
        from(bucket:"iot_stream_data")
            |> range(start: -{flux_range})
            |> filter(fn: (r) => r._measurement == "sensor_measurement")
            |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        
        tables = self.query_api.query(query)
        
        data = []
        for table in tables:
            for record in table.records:
                data.append({
                    'time': record.get_time(),
                    'sensor_id': record.values.get('sensor_id'),
                    'sensor_type': record.values.get('sensor_type'),
                    'location': record.values.get('location', 'unknown'),
                    'value': record.values.get('value'),
                    'is_anomaly': record.values.get('is_anomaly', False)
                })
        
        df = pd.DataFrame(data)
        print(f" Retrieved {len(df)} data points from InfluxDB")
        return df
    
    # -----------------------------
    # Basic Statistics
    # -----------------------------
    def basic_stats(self, df):
        print("\n" + "="*60)
        print(" BASIC STATISTICS")
        print("="*60)
        
        print(f"\n Dataset Overview:")
        print(f"   - Total records: {len(df)}")
        print(f"   - Time range: {df['time'].min()} to {df['time'].max()}")
        print(f"   - Number of sensors: {df['sensor_id'].nunique()}")
        print(f"   - Sensor types: {df['sensor_type'].unique().tolist()}")
        print(f"   - Locations: {df['location'].unique().tolist()}")
        
        print(f"\n Value Statistics by Sensor Type:")
        for sensor in df['sensor_type'].unique():
            sensor_data = df[df['sensor_type'] == sensor]['value']
            print(f"\n   {sensor.upper()}:")
            print(f"      Count: {len(sensor_data)}")
            print(f"      Mean: {sensor_data.mean():.2f}")
            print(f"      Std: {sensor_data.std():.2f}")
            print(f"      Min: {sensor_data.min():.2f}")
            print(f"      Max: {sensor_data.max():.2f}")
            print(f"      Q1: {sensor_data.quantile(0.25):.2f}")
            print(f"      Median: {sensor_data.median():.2f}")
            print(f"      Q3: {sensor_data.quantile(0.75):.2f}")
        
        print(f"\n Anomaly Statistics:")
        anomalies = df[df['is_anomaly'] == True]
        print(f"   - Total anomalies: {len(anomalies)}")
        if len(df) > 0:
            print(f"   - Anomaly rate: {(len(anomalies)/len(df)*100):.2f}%")
        
        if len(anomalies) > 0:
            print(f"\n   Anomalies by sensor type:")
            for sensor in anomalies['sensor_type'].unique():
                count = len(anomalies[anomalies['sensor_type'] == sensor])
                print(f"      {sensor}: {count} anomalies")
    
    # -----------------------------
    # Plotting functions (Corrected)
    # -----------------------------
    def plot_time_series(self, df):
        sensors = df['sensor_type'].unique()
        n_sensors = len(sensors)
        
        fig, axes = plt.subplots(n_sensors, 1, figsize=(15, 5*n_sensors))
        if n_sensors == 1:
            axes = [axes]
        
        fig.suptitle(' Sensor Readings Over Time', fontsize=16)
        
        for idx, sensor in enumerate(sensors):
            sensor_data = df[df['sensor_type'] == sensor].sort_values('time')
            
            if len(sensor_data) > 0:
                axes[idx].plot(sensor_data['time'], sensor_data['value'], 
                              label=sensor, color='blue', alpha=0.7, linewidth=1)
                
                anomalies = sensor_data[sensor_data['is_anomaly'] == True]
                if len(anomalies) > 0:
                    axes[idx].scatter(anomalies['time'], anomalies['value'], 
                                     color='red', s=50, label='Anomalies', zorder=5, alpha=0.8)
            
            axes[idx].set_title(f'{sensor.upper()} Sensor')
            axes[idx].set_ylabel('Value')
            axes[idx].legend(loc='upper right')
            axes[idx].grid(True, alpha=0.3)
            axes[idx].tick_params(axis='x', rotation=45)
        
        axes[-1].set_xlabel('Time')
        plt.tight_layout()
        plt.show()
    
    def plot_distributions(self, df):
        sensors = df['sensor_type'].unique()
        n_sensors = len(sensors)
        
        fig, axes = plt.subplots(2, n_sensors, figsize=(6*n_sensors, 10))
        if n_sensors == 1:
            axes = axes.reshape(2, 1)
        
        fig.suptitle(' Value Distributions by Sensor Type', fontsize=16)
        
        for idx, sensor in enumerate(sensors):
            sensor_data = df[df['sensor_type'] == sensor]['value']
            # Histogram
            axes[0, idx].hist(sensor_data, bins=30, edgecolor='black', alpha=0.7, color='skyblue')
            axes[0, idx].set_title(f'{sensor.upper()} - Distribution')
            axes[0, idx].set_xlabel('Value')
            axes[0, idx].set_ylabel('Frequency')
            axes[0, idx].axvline(sensor_data.mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {sensor_data.mean():.2f}')
            axes[0, idx].axvline(sensor_data.median(), color='green', linestyle='--', linewidth=2, label=f'Median: {sensor_data.median():.2f}')
            axes[0, idx].legend()
            axes[0, idx].grid(True, alpha=0.3)
            # Box plot
            box_data = [sensor_data]
            bp = axes[1, idx].boxplot(box_data, patch_artist=True)
            bp['boxes'][0].set_facecolor('lightblue')
            axes[1, idx].set_title(f'{sensor.upper()} - Box Plot')
            axes[1, idx].set_ylabel('Value')
            axes[1, idx].set_xticklabels([sensor])
            axes[1, idx].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
    
    def plot_correlations(self, df):
        """Corrected version of correlations function"""
        # Prepare data for correlation
        pivot_df = df.pivot_table(
            index=df['time'].dt.round('1min'),  # Round time to minute
            columns='sensor_type', 
            values='value', 
            aggfunc='mean'  # Take average per minute
        )
        
        # Remove columns with too many missing values
        pivot_df = pivot_df.dropna(axis=1, thresh=len(pivot_df)*0.5)
        
        if len(pivot_df.columns) < 2:
            print(" Not enough sensor types for correlation analysis")
            return None
        
        # Calculate correlation matrix
        corr_matrix = pivot_df.corr()
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        
        # Heatmap
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, 
                   square=True, ax=axes[0], fmt='.2f', cbar_kws={'shrink':0.8})
        axes[0].set_title(' Sensor Correlation Matrix', fontsize=14)
        
        # Scatter plots - plot each pair separately
        colors = ['red', 'blue', 'green', 'orange', 'purple']
        for i, (sensor1, sensor2) in enumerate([(pivot_df.columns[0], pivot_df.columns[1])] 
                                               if len(pivot_df.columns) >= 2 else []):
            if i < len(colors):
                axes[1].scatter(pivot_df[sensor1], pivot_df[sensor2], 
                              alpha=0.5, s=30, color=colors[i], 
                              label=f'{sensor1}-{sensor2}')
        
        axes[1].set_xlabel('Sensor Value', fontsize=12)
        axes[1].set_ylabel('Sensor Value', fontsize=12)
        axes[1].set_title(' Sensor Relationships', fontsize=14)
        if len(pivot_df.columns) >= 2:
            axes[1].legend(loc='center left', bbox_to_anchor=(1, 0.5))
        axes[1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.show()
        return corr_matrix
    
    def anomaly_patterns(self, df):
        print("\n" + "="*60)
        print(" ANOMALY PATTERNS")
        print("="*60)
        
        anomalies = df[df['is_anomaly'] == True].copy()
        if len(anomalies) == 0:
            print("No anomalies found in the data period")
            return
        
        print(f"\n Anomalies by Location:")
        for loc in anomalies['location'].unique():
            loc_anomalies = anomalies[anomalies['location'] == loc]
            print(f"   {loc}: {len(loc_anomalies)} anomalies")
        
        anomalies['hour'] = pd.to_datetime(anomalies['time']).dt.hour
        hourly = anomalies.groupby('hour').size()
        
        fig, axes = plt.subplots(1, 2, figsize=(14,6))
        axes[0].bar(hourly.index, hourly.values, color='coral', edgecolor='black', alpha=0.7)
        axes[0].set_xlabel('Hour of Day', fontsize=12)
        axes[0].set_ylabel('Number of Anomalies', fontsize=12)
        axes[0].set_title(' Anomalies by Hour of Day', fontsize=14)
        axes[0].set_xticks(range(0,24,2))
        axes[0].grid(True, alpha=0.3, axis='y')
        
        sensor_counts = anomalies['sensor_type'].value_counts()
        colors = plt.cm.Set3(range(len(sensor_counts)))
        axes[1].pie(sensor_counts.values, labels=sensor_counts.index, autopct='%1.1f%%', colors=colors, startangle=90, explode=[0.05]*len(sensor_counts))
        axes[1].set_title(' Anomalies by Sensor Type', fontsize=14)
        
        plt.tight_layout()
        plt.show()
        
        print(f"\n Anomaly Value Statistics:")
        for sensor in anomalies['sensor_type'].unique():
            sensor_anomalies = anomalies[anomalies['sensor_type'] == sensor]
            print(f"\n   {sensor.upper()}:")
            print(f"      Count: {len(sensor_anomalies)}")
            print(f"      Mean value: {sensor_anomalies['value'].mean():.2f}")
            print(f"      Max value: {sensor_anomalies['value'].max():.2f}")
            print(f"      Min value: {sensor_anomalies['value'].min():.2f}")
    
    def generate_report(self, df):
        """Generate comprehensive report with additional data"""
        print("\n" + "="*60)
        print(" COMPREHENSIVE EDA REPORT")
        print("="*60)
        self.basic_stats(df)
        
        # Additional hourly analysis
        df['hour'] = pd.to_datetime(df['time']).dt.hour
        df['minute'] = pd.to_datetime(df['time']).dt.minute
        
        hourly_avg = df.groupby(['hour','sensor_type'])['value'].mean().unstack()
        print("\n Hourly Averages:")
        print(hourly_avg.round(2))
        
        print("\n Hourly Statistics:")
        for sensor in df['sensor_type'].unique():
            sensor_hourly = df[df['sensor_type'] == sensor].groupby('hour')['value'].agg(['mean', 'max', 'count'])
            print(f"\n   {sensor.upper()}:")
            if len(sensor_hourly) > 0:
                print(f"      Peak hour: {sensor_hourly['mean'].idxmax()}:00 (mean: {sensor_hourly['mean'].max():.2f})")
                print(f"      Busiest hour: {sensor_hourly['count'].idxmax()}:00 ({sensor_hourly['count'].max()} readings)")
    
    def close(self):
        if self.client:
            self.client.close()

# -----------------------------
# Main execution with CSV save
# -----------------------------
if __name__ == "__main__":
    print(" Starting IoT Data EDA (Live Stream)...")
    print("="*60)
    
    # 1 Get data from Kafka
    df = get_stream_data(
        topic='raw-sensor-data',  # Correction: use correct topic name
        bootstrap_servers='localhost:9092', 
        max_messages=3000
    )

    if len(df) == 0:
        print(" No data found in Kafka! Make sure sensors are running and sending data.")
        print("\n Troubleshooting tips:")
        print("   1. Check if Kafka is running: docker ps")
        print("   2. Check if sensor simulator is running")
        print("   3. Check Kafka topics: docker exec kafka kafka-topics --list --bootstrap-server localhost:9092")
    else:
        # 2 Save data to CSV
        print("\n" + "="*60)
        print(" SAVING CLEANED DATA TO CSV")
        print("="*60)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'cleaned_iot_data_{timestamp}.csv'
        
        # Save data
        save_to_csv(df, filename)
        
        # 3 Analyze data
        analyzer = IoTDataAnalysis(token=None)
        try:
            df = analyzer.get_sensor_data(df=df)
            analyzer.generate_report(df)
            
            print("\n Generating visualizations...")
            analyzer.plot_time_series(df)
            analyzer.plot_distributions(df)
            analyzer.plot_correlations(df)
            analyzer.anomaly_patterns(df)
            
            print("\n EDA Complete!")
            print(f"   Analyzed {len(df)} data points from {df['sensor_type'].nunique()} sensor types")
            
        finally:
            analyzer.close()