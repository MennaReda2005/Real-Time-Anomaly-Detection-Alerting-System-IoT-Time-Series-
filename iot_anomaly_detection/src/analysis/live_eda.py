# live_eda.py
from kafka import KafkaConsumer
import json
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import time
from EDA_analysis import IoTDataAnalysis, get_stream_data, save_to_csv
import os

class LiveEDAAnalyzer:
    def __init__(self, save_interval=10):
        """
        Initialize Live EDA Analyzer
        
        Args:
            save_interval: Number of cycles between CSV saves
        """
        self.analyzer = IoTDataAnalysis(token=None)
        self.all_data = []  # Accumulate all data
        self.cycle_count = 0
        self.save_interval = save_interval
        
    def run_live_eda(self, interval_minutes=1, max_messages=100):
        """
        Run live EDA periodically
        
        Args:
            interval_minutes: Minutes between analysis cycles
            max_messages: Maximum messages to fetch per cycle
        """
        print(" Starting Live EDA System")
        print("="*60)
        print(f" Interval: {interval_minutes} minute(s)")
        print(f" Max messages per cycle: {max_messages}")
        print(f" CSV save interval: Every {self.save_interval} cycles")
        print("="*60)
        
        try:
            while True:
                self.cycle_count += 1
                print("\n" + "="*70)
                print(f" Live EDA Cycle #{self.cycle_count} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*70)
                
                # 1 Fetch latest data
                df_new = get_stream_data(
                    topic='raw-sensor-data',
                    max_messages=max_messages
                )
                
                if len(df_new) > 0:
                    # 2 Add to accumulated data
                    self.all_data.append(df_new)
                    
                    # 3 Analyze new data
                    print(f"\n Analyzing {len(df_new)} new records...")
                    df_analyzed = self.analyzer.get_sensor_data(df=df_new)
                    
                    # 4 Show quick stats
                    self.show_quick_stats(df_new)
                    
                    # 5 Quick plot
                    self.plot_live_summary(df_new)
                    
                    # 6 Save data periodically
                    if self.cycle_count % self.save_interval == 0:
                        self.save_accumulated_data()
                else:
                    print(" No new data received in this cycle")
                
                # 7 Wait for next cycle
                print(f"\n Waiting {interval_minutes} minute(s) for next cycle...")
                time.sleep(interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\n Live EDA stopped by user")
            self.save_accumulated_data(final=True)
        finally:
            self.analyzer.close()
    
    def show_quick_stats(self, df):
        """Show quick statistics for the latest batch"""
        print("\n Quick Stats (Last Batch):")
        print(f"   - Sensors: {df['sensor_type'].unique().tolist()}")
        print(f"   - Time range: {df['time'].min().strftime('%H:%M:%S')} - {df['time'].max().strftime('%H:%M:%S')}")
        
        anomalies = df[df['is_anomaly'] == True]
        if len(anomalies) > 0:
            print(f"    Anomalies detected: {len(anomalies)}")
            for sensor in anomalies['sensor_type'].unique():
                count = len(anomalies[anomalies['sensor_type'] == sensor])
                print(f"      - {sensor}: {count}")
    
    def plot_live_summary(self, df):
        """Quick plot for the latest data"""
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        
        # Plot values over time
        for sensor in df['sensor_type'].unique():
            sensor_data = df[df['sensor_type'] == sensor].sort_values('time')
            if len(sensor_data) > 0:
                axes[0].plot(sensor_data['time'], sensor_data['value'], 
                            'o-', label=sensor, alpha=0.7, markersize=4)
        
        axes[0].set_title('Latest Sensor Readings')
        axes[0].set_xlabel('Time')
        axes[0].set_ylabel('Value')
        axes[0].legend()
        axes[0].tick_params(axis='x', rotation=45)
        
        # Plot value distribution
        for sensor in df['sensor_type'].unique():
            sensor_data = df[df['sensor_type'] == sensor]['value']
            if len(sensor_data) > 0:
                axes[1].hist(sensor_data, alpha=0.5, label=sensor, bins=10)
        
        axes[1].set_title('Value Distribution')
        axes[1].set_xlabel('Value')
        axes[1].set_ylabel('Frequency')
        axes[1].legend()
        
        plt.tight_layout()
        plt.show()
    
    def save_accumulated_data(self, final=False):
        """Save all accumulated data to CSV"""
        if not self.all_data:
            print(" No data to save")
            return
        
        # Combine all data
        df_combined = pd.concat(self.all_data, ignore_index=True)
        
        # Remove duplicates
        df_combined = df_combined.drop_duplicates(subset=['sensor_id', 'time']).sort_values('time')
        
        # Generate filename
        if final:
            filename = f'live_eda_final_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        else:
            filename = f'live_eda_cycle_{self.cycle_count}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        # Save data
        save_to_csv(df_combined, filename)
        print(f" Accumulated data saved ({len(df_combined)} total records)")

# -----------------------------
# Main Execution
# -----------------------------
if __name__ == "__main__":
    print("="*60)
    print(" LIVE EDA SYSTEM")
    print("="*60)
    
    # Choose operation mode
    print("\n Choose mode:")
    print("1. Single analysis (one-time with CSV save)")
    print("2. Live analysis (continuous with periodic CSV save)")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        # Run single analysis
        print("\n Running single analysis...")
        df = get_stream_data(max_messages=3000)
        if len(df) > 0:
            save_to_csv(df, f'analysis_output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
            analyzer = IoTDataAnalysis(token=None)
            df_analyzed = analyzer.get_sensor_data(df=df)
            analyzer.generate_report(df_analyzed)
            
            print("\n Generating visualizations...")
            analyzer.plot_time_series(df_analyzed)
            analyzer.plot_distributions(df_analyzed)
            analyzer.plot_correlations(df_analyzed)
            analyzer.anomaly_patterns(df_analyzed)
            
            analyzer.close()
    
    elif choice == "2":
        # Run live analysis
        interval = input("Enter interval in minutes (default=1): ").strip()
        interval = int(interval) if interval else 1
        
        max_msgs = input("Enter max messages per cycle (default=100): ").strip()
        max_msgs = int(max_msgs) if max_msgs else 100
        
        save_int = input("Enter CSV save interval in cycles (default=5): ").strip()
        save_int = int(save_int) if save_int else 5
        
        live_analyzer = LiveEDAAnalyzer(save_interval=save_int)
        live_analyzer.run_live_eda(interval_minutes=interval, max_messages=max_msgs)
    
    else:
        print(" Invalid choice")