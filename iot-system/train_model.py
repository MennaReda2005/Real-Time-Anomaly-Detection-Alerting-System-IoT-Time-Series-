# -*- coding: utf-8 -*-
"""
ADVANCED IOT ANOMALY DETECTION - OPTIMIZED VERSION (FIXED)
تم إصلاح مشكلة التوافق مع TensorFlow 2.x
"""

import os
import traceback
import numpy as np
import pandas as pd
import joblib
import random
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import LSTM, Dense, Dropout, Input, RepeatVector, TimeDistributed, LayerNormalization
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
from sklearn.preprocessing import StandardScaler
from scipy.ndimage import gaussian_filter1d
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# تعيين البذور العشوائية للتكرار
random.seed(42)
np.random.seed(42)
tf.random.set_seed(42)

# إعدادات التسجيل
tf.get_logger().setLevel('ERROR')
tf.keras.utils.disable_interactive_logging()

print("=" * 60)
print("🔧 ADVANCED IOT ANOMALY DETECTION - OPTIMIZED VERSION (FIXED)")
print("=" * 60)

# ============================================================
# الإعدادات العامة
# ============================================================
SENSOR_TYPES = ['temperature', 'humidity', 'gas', 'vibration', 'smoke']
WINDOW_SIZE = 5
EPOCHS = 100
BATCH_SIZE = 32

# إنشاء المجلدات
os.makedirs('models', exist_ok=True)
os.makedirs('scalers', exist_ok=True)
os.makedirs('results', exist_ok=True)

# مسار ملف البيانات
csv_file = 'sensor_data_20260708_032400.csv'


# ============================================================
# 1. تحسين معالجة البيانات (Data Processing)
# ============================================================
def advanced_preprocessing(data, sensor_type):
    """
    معالجة متقدمة للبيانات حسب نوع المستشعر
    """
    data = np.array(data, dtype=float)
    
    # 1. معالجة حسب نوع المستشعر
    if sensor_type == 'humidity':
        data = np.clip(data, 0, 100)
    elif sensor_type == 'vibration':
        data = np.maximum(data, 0)
        zero_threshold = 0.05
        data[data < zero_threshold] = zero_threshold
    elif sensor_type == 'gas':
        data = np.maximum(data, 0)
    elif sensor_type == 'smoke':
        data = np.clip(data, 0, 100)
    elif sensor_type == 'temperature':
        data = np.clip(data, -10, 60)
    
    # 2. تنعيم البيانات
    if len(data) > 10:
        try:
            data = gaussian_filter1d(data, sigma=0.5)
        except:
            pass
    
    return data


def load_and_prepare_data(csv_file):
    """تحميل وإعداد البيانات من ملف CSV"""
    df = pd.read_csv(csv_file)
    required_cols = {'timestamp', 'sensor_id', 'sensor_type', 'value', 'unit'}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")
    
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(f"✅ Loaded {len(df)} records from {csv_file}")
    return df


def extract_normal_data(df, sensor_type):
    """استخراج بيانات عادية لنوع مستشعر معين"""
    sensor_df = df[df['sensor_type'] == sensor_type].copy()
    sensor_df = sensor_df.sort_values('timestamp')
    normal_data = sensor_df['value'].values.astype(float)
    return normal_data


# ============================================================
# 2. تحسين هيكل النموذج (Architecture)
# ============================================================
def build_adaptive_autoencoder(sensor_type, window_size, n_features=1):
    """
    بناء نموذج مخصص لكل نوع مستشعر
    """
    inputs = Input(shape=(window_size, n_features))
    
    # اختيار الهيكل المناسب حسب نوع المستشعر
    if sensor_type in ['humidity', 'gas']:
        lstm_units = [64, 32, 16]
        dropout_rate = 0.3
        activation = 'tanh'
    elif sensor_type == 'vibration':
        lstm_units = [48, 24, 12]
        dropout_rate = 0.25
        activation = 'relu'
    else:  # temperature, smoke
        lstm_units = [32, 16, 8]
        dropout_rate = 0.2
        activation = 'relu'
    
    # ===== Encoder =====
    encoded = inputs
    for units in lstm_units[:-1]:
        encoded = LSTM(units, activation=activation, return_sequences=True)(encoded)
        encoded = LayerNormalization()(encoded)
        encoded = Dropout(dropout_rate)(encoded)
    
    # آخر طبقة LSTM (بدون return sequences)
    encoded = LSTM(lstm_units[-1], activation=activation, return_sequences=False)(encoded)
    encoded = LayerNormalization()(encoded)
    encoded = Dropout(dropout_rate)(encoded)
    
    # ===== Decoder =====
    decoded = RepeatVector(window_size)(encoded)
    
    for units in reversed(lstm_units[:-1]):
        decoded = LSTM(units, activation=activation, return_sequences=True)(decoded)
        decoded = LayerNormalization()(decoded)
        decoded = Dropout(dropout_rate)(decoded)
    
    decoded = TimeDistributed(Dense(n_features, activation='linear'))(decoded)
    
    autoencoder = Model(inputs, decoded)
    
    # معدل تعلم مخصص لكل نوع
    if sensor_type in ['humidity', 'gas']:
        lr = 0.0005
    else:
        lr = 0.001
    
    autoencoder.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=lr),
        loss='mae'
    )
    
    return autoencoder


# ============================================================
# 3. تحسين حساب العتبات (Thresholds)
# ============================================================
def calculate_adaptive_threshold(errors, sensor_type):
    """
    حساب عتبة متكيفة مع نوع المستشعر
    """
    errors = np.array(errors)
    
    p95 = np.percentile(errors, 95)
    p99 = np.percentile(errors, 99)
    q1 = np.percentile(errors, 25)
    q3 = np.percentile(errors, 75)
    iqr = q3 - q1
    mean = np.mean(errors)
    std = np.std(errors)
    
    threshold_percentile = p99
    threshold_iqr = q3 + 1.5 * iqr
    threshold_mean_std = mean + 3 * std
    
    # اختيار الطريقة المناسبة حسب نوع المستشعر
    if sensor_type in ['vibration', 'gas']:
        threshold = min(threshold_percentile * 0.95, threshold_iqr * 1.2)
    elif sensor_type == 'humidity':
        threshold = max(threshold_percentile, threshold_iqr)
    else:
        threshold = min(threshold_percentile, threshold_iqr * 1.1)
    
    # ضمان عدم انخفاض العتبة بشكل مبالغ فيه
    min_threshold = mean + 2 * std
    threshold = max(threshold, min_threshold)
    
    print(f"  📊 Threshold Analysis for {sensor_type}:")
    print(f"     - Percentile (99%): {p99:.6f}")
    print(f"     - IQR method: {threshold_iqr:.6f}")
    print(f"     - Mean+3Std: {threshold_mean_std:.6f}")
    print(f"     - ✅ Final threshold: {threshold:.6f}")
    
    return threshold


# ============================================================
# 4. الكشف المتقدم عن الشذوذ (Ensemble Detection)
# ============================================================
def ensemble_anomaly_detection(value, error, threshold, sensor_type, history_values=None):
    """
    نظام كشف متكامل يستخدم عدة طرق للكشف عن الشذوذ
    """
    # 1. الكشف الأساسي (MAE)
    is_anomaly_mae = error > threshold
    
    # 2. الكشف بناءً على القيم الطبيعية للمستشعر
    is_anomaly_range = False
    if sensor_type == 'humidity':
        if value < 0 or value > 100:
            is_anomaly_range = True
    elif sensor_type == 'vibration':
        if value > 50 or value < 0:
            is_anomaly_range = True
    elif sensor_type == 'gas':
        if value < 0:
            is_anomaly_range = True
    elif sensor_type == 'smoke':
        if value < 0 or value > 100:
            is_anomaly_range = True
    elif sensor_type == 'temperature':
        if value < -10 or value > 60:
            is_anomaly_range = True
    
    # 3. الكشف بناءً على نسبة الخطأ
    error_ratio = error / threshold if threshold > 0 else float('inf')
    is_anomaly_ratio = error_ratio > 2.0
    
    # 4. استثناءات للقيم الطبيعية
    is_exception = False
    if sensor_type == 'gas' and value == 0:
        is_exception = True
    elif sensor_type == 'vibration' and value < 0.1:
        is_exception = True
    elif sensor_type == 'smoke' and value < 1:
        is_exception = True
    
    # 5. اتخاذ القرار النهائي
    if is_exception:
        return False, error_ratio
    
    if error_ratio > 3.0:
        return True, error_ratio
    
    # التصويت المرجح
    votes = 0
    if is_anomaly_mae:
        votes += 1
    if is_anomaly_range:
        votes += 2
    if is_anomaly_ratio:
        votes += 1
    
    is_anomaly = votes >= 2
    
    return is_anomaly, error_ratio


# ============================================================
# 5. إنشاء تقرير الأداء
# ============================================================
def generate_model_report(sensor_type, history, threshold, errors_stats):
    """إنشاء تقرير مفصل عن أداء النموذج"""
    report_path = f'results/report_{sensor_type}.txt'
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write(f"📊 MODEL PERFORMANCE REPORT: {sensor_type.upper()}\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("📈 TRAINING METRICS:\n")
        f.write(f"  Final Training Loss: {history.history['loss'][-1]:.6f}\n")
        f.write(f"  Final Validation Loss: {history.history['val_loss'][-1]:.6f}\n")
        f.write(f"  Best Validation Loss: {min(history.history['val_loss']):.6f}\n")
        f.write(f"  Total Epochs: {len(history.history['loss'])}\n\n")
        
        f.write("🎯 THRESHOLD INFORMATION:\n")
        f.write(f"  Detection Threshold: {threshold:.6f}\n")
        f.write(f"  Error Statistics:\n")
        f.write(f"    Min: {errors_stats['min']:.6f}\n")
        f.write(f"    Max: {errors_stats['max']:.6f}\n")
        f.write(f"    Mean: {errors_stats['mean']:.6f}\n")
        f.write(f"    Std: {errors_stats['std']:.6f}\n")
        f.write(f"    Median: {errors_stats['median']:.6f}\n")
        f.write(f"    Q1: {errors_stats['q1']:.6f}\n")
        f.write(f"    Q3: {errors_stats['q3']:.6f}\n\n")
        
        f.write("💡 RECOMMENDATIONS:\n")
        if threshold > errors_stats['mean'] + 4 * errors_stats['std']:
            f.write("  ⚠️ العتبة مرتفعة جداً - قد تفوت حالات شاذة\n")
        elif threshold < errors_stats['mean'] + 2 * errors_stats['std']:
            f.write("  ⚠️ العتبة منخفضة جداً - قد تعطي إنذارات خاطئة كثيرة\n")
        else:
            f.write("  ✅ العتبة في نطاق جيد\n")
    
    print(f"  ✅ Report saved: {report_path}")


# ============================================================
# 6. دوال مساعدة
# ============================================================
def create_sequences(data, window_size):
    """إنشاء تسلسلات من البيانات"""
    X = []
    for i in range(len(data) - window_size + 1):
        X.append(data[i:i + window_size])
    return np.array(X)


def train_model(model, X_train, X_val, sensor_type):
    """
    تدريب النموذج - تم إصلاح مشكلة التوافق
    """
    # 1. Early stopping
    early_stop = EarlyStopping(
        monitor='val_loss',
        patience=25,
        restore_best_weights=True,
        min_delta=1e-5
    )
    
    # 2. Reduce LR
    reduce_lr = ReduceLROnPlateau(
        monitor='val_loss',
        factor=0.5,
        patience=12,
        min_lr=1e-7,
        verbose=0
    )
    
    # 3. تدريب النموذج (بدون ModelCheckpoint لتجنب مشاكل التوافق)
    history = model.fit(
        X_train, X_train,
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,
        validation_data=(X_val, X_val),
        callbacks=[early_stop, reduce_lr],
        verbose=1
    )
    
    return model, history


# ============================================================
# 7. الوظيفة الرئيسية
# ============================================================
def main():
    print("\n📂 Loading and preparing data from CSV...\n")
    print(f"Current working directory: {os.getcwd()}")
    
    try:
        df = load_and_prepare_data(csv_file)
    except FileNotFoundError:
        print(f"❌ Error: File '{csv_file}' not found!")
        print("Please update the 'csv_file' variable with the correct path.")
        return
    
    print("\n📊 Sensor types found in the file:")
    print(df['sensor_type'].value_counts())
    print()
    
    all_models = {}
    all_scalers = {}
    all_thresholds = {}
    
    for sensor_type in SENSOR_TYPES:
        print(f"\n{'='*50}")
        print(f"🎯 Training: {sensor_type.upper()} Sensor")
        print(f"{'='*50}")
        
        try:
            # 1. استخراج البيانات
            normal_data = extract_normal_data(df, sensor_type)
            print(f"  📊 Normal samples: {len(normal_data)}")
            
            if len(normal_data) < WINDOW_SIZE + 1:
                print(f"  ⚠️ Not enough normal data for {sensor_type} (need > {WINDOW_SIZE}). Skipping...")
                continue
            
            # 2. معالجة متقدمة للبيانات
            normal_data = advanced_preprocessing(normal_data, sensor_type)
            
            # 3. إنشاء التسلسلات
            X_seq = create_sequences(normal_data, WINDOW_SIZE)
            X_seq = X_seq.reshape(-1, WINDOW_SIZE, 1)
            
            # 4. تقسيم البيانات
            split = int(len(X_seq) * 0.8)
            X_train = X_seq[:split]
            X_val = X_seq[split:]
            
            if len(X_val) == 0:
                print(f"  ⚠️ Not enough data for validation split. Skipping...")
                continue
            
            # 5. تطبيق StandardScaler
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train.reshape(-1, 1)).reshape(-1, WINDOW_SIZE, 1)
            X_val_scaled = scaler.transform(X_val.reshape(-1, 1)).reshape(-1, WINDOW_SIZE, 1)
            
            print(f"  📊 Training samples: {len(X_train)} (ALL NORMAL)")
            print(f"  📊 Validation samples: {len(X_val)} (ALL NORMAL)")
            
            # 6. بناء النموذج المتكيف
            model = build_adaptive_autoencoder(sensor_type, WINDOW_SIZE)
            
            # 7. حفظ هيكل النموذج
            with open(f'results/{sensor_type}_summary.txt', 'w') as f:
                model.summary(print_fn=lambda x: f.write(x + '\n'))
            
            # 8. التدريب
            print(f"  🚀 Training on NORMAL data only...")
            model, history = train_model(model, X_train_scaled, X_val_scaled, sensor_type)
            
            # 9. حساب أخطاء إعادة البناء
            train_reconstructions = model.predict(X_train_scaled, verbose=0)
            per_point_errors = np.abs(train_reconstructions - X_train_scaled).mean(axis=2)
            last_point_errors = per_point_errors[:, -1]
            
            # 10. حساب العتبة المتكيفة
            threshold = calculate_adaptive_threshold(last_point_errors, sensor_type)
            
            # 11. حفظ النموذج والـ scaler والعتبة
            model.save(f'models/lstm_autoencoder_{sensor_type}.keras')
            joblib.dump(scaler, f'scalers/scaler_{sensor_type}.pkl')
            joblib.dump(threshold, f"models/{sensor_type}_threshold.pkl")
            
            with open(f"models/{sensor_type}_threshold.txt", "w") as f:
                f.write(str(threshold))
            
            # 12. حفظ إحصائيات الأخطاء
            error_stats = {
                'min': np.min(last_point_errors),
                'max': np.max(last_point_errors),
                'mean': np.mean(last_point_errors),
                'std': np.std(last_point_errors),
                'median': np.median(last_point_errors),
                'q1': np.percentile(last_point_errors, 25),
                'q3': np.percentile(last_point_errors, 75)
            }
            
            # 13. إنشاء تقرير الأداء
            generate_model_report(sensor_type, history, threshold, error_stats)
            
            # 14. حفظ معلومات التدريب
            history_df = pd.DataFrame(history.history)
            history_df.to_csv(f'results/history_{sensor_type}.csv', index=False)
            
            # 15. رسم منحنى التدريب
            plt.figure(figsize=(10, 6))
            plt.plot(history.history['loss'], label='Training Loss', linewidth=2)
            plt.plot(history.history['val_loss'], label='Validation Loss', linewidth=2)
            plt.title(f'Training History - {sensor_type.upper()}')
            plt.xlabel('Epoch')
            plt.ylabel('MAE Loss')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.savefig(f'results/training_history_{sensor_type}.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            all_models[sensor_type] = model
            all_scalers[sensor_type] = scaler
            all_thresholds[sensor_type] = threshold
            
            print(f"  ✅ Model trained successfully!")
            print(f"     - Final loss: {history.history['loss'][-1]:.6f}")
            print(f"     - Validation loss: {history.history['val_loss'][-1]:.6f}")
            print(f"     - Threshold: {threshold:.6f}")
            
        except KeyboardInterrupt:
            print(f"  ⚠️ Training interrupted by user for {sensor_type}")
            break
        except Exception as e:
            print(f"  ❌ Error training {sensor_type}: {e}")
            traceback.print_exc()
    
    # ============================================================
    # 8. التقرير النهائي
    # ============================================================
    print("\n" + "=" * 60)
    print("🎉 TRAINING COMPLETED!")
    print("=" * 60)
    
    print("\n📁 Generated Files:")
    for folder in ['models', 'scalers', 'results']:
        print(f"\n  📂 {folder}/:")
        files = os.listdir(folder)
        if files:
            for f in sorted(files):
                size = os.path.getsize(os.path.join(folder, f))
                print(f"    - {f} ({size:,} bytes)")
        else:
            print("    (Empty)")
    
    # طباعة ملخص العتبات
    if all_thresholds:
        print("\n" + "=" * 60)
        print("📊 THRESHOLD SUMMARY")
        print("=" * 60)
        for sensor_type, threshold in all_thresholds.items():
            print(f"  {sensor_type.upper():12} : {threshold:.6f}")
    
    print("\n✅ All models saved successfully!")
    print("📝 Check 'results/report_*.txt' for detailed performance reports.")


# ============================================================
# 9. تنفيذ البرنامج
# ============================================================
if __name__ == "__main__":
    main()