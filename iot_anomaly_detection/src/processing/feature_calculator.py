import statistics
from collections import deque
from datetime import datetime

class FeatureCalculator:
    def __init__(self, window_size=10):
        self.window_size = window_size
        self.data_windows = {}
    
    def update_window(self, sensor_key, value):
        """Update time window for sensor"""
        if sensor_key not in self.data_windows:
            self.data_windows[sensor_key] = deque(maxlen=self.window_size)
        
        self.data_windows[sensor_key].append({
            'value': value,
            'timestamp': datetime.now()
        })
        
        return list(self.data_windows[sensor_key])
    
    def calculate_features(self, window_data):
        """Calculate all features from time window"""
        if len(window_data) < 3:
            return {}
        
        values = [item['value'] for item in window_data]
        timestamps = [item['timestamp'] for item in window_data]
        
        # Basic features
        features = {
            'count': len(values),
            'mean': statistics.mean(values),
            'min': min(values),
            'max': max(values),
            'range': max(values) - min(values),
        }
        
        # Statistical features (need at least 2 values)
        if len(values) >= 2:
            features['std'] = statistics.stdev(values)
            features['variance'] = statistics.variance(values)
        else:
            features['std'] = 0
            features['variance'] = 0
        
        # Rate of change (if we have enough values)
        if len(values) >= 2:
            time_diff = (timestamps[-1] - timestamps[0]).total_seconds()
            if time_diff > 0:
                value_diff = values[-1] - values[0]
                features['rate_of_change'] = value_diff / time_diff
            else:
                features['rate_of_change'] = 0
        
        # Moving window features
        if len(values) >= 5:
            # Moving average (5 points)
            moving_avg = statistics.mean(values[-5:])
            features['moving_avg_5'] = moving_avg
            
            # Is current value above moving average?
            features['above_moving_avg'] = values[-1] > moving_avg * 1.2
        
        # Signal if window is complete
        features['window_complete'] = len(window_data) == self.window_size
        
        return features