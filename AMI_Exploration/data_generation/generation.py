import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import AMI_Exploration.db.db as db

# Parameters
n_meters_per_phase = 80
n_phases = 3
n_meters = n_meters_per_phase * n_phases
intervals = 96  # 24 hours at 15-minute intervals

# Time index and base waveform
t = np.arange(intervals)
base_daily = 239 + 0.5 * np.sin(2 * np.pi * t / intervals * 2)  # small daily sinus variation
phase_shifts = [0, 0.8, -0.7]  # offsets to differentiate phases

# Create reference phase waveforms
phase_refs = []
for i in range(n_phases):
    ref = base_daily + phase_shifts[i] + 0.2 * np.sin(2 * np.pi * t / 24)
    ref += 0.05 * np.random.randn(intervals)
    phase_refs.append(ref)
phase_refs = np.array(phase_refs)  # shape (3, intervals)

# Generate meter signals
meters = []
labels = []
event_counts = []
avg_dip_mags = []
for p in range(n_phases):
    for m in range(n_meters_per_phase):
        signal = phase_refs[p].copy()
        signal += 0.2 * np.random.randn(intervals)  # measurement noise
        # occasional transient dip
        dips = 0
        dip_mags = []
        if np.random.rand() < 0.35:
            dip_start = np.random.randint(10, intervals-10)
            dip_len = np.random.randint(1, 6)
            mag = np.random.uniform(0.5, 2.5)
            signal[dip_start:dip_start+dip_len] -= mag
            dips += 1
            dip_mags.append(mag)
        # additional small events with low probability
        for _ in range(np.random.poisson(0.2)):
            i0 = np.random.randint(0, intervals-3)
            signal[i0:i0+2] -= np.random.uniform(0.3, 1.0)
            dips += 1
            dip_mags.append(np.random.uniform(0.3,1.0))
        meters.append(signal)
        labels.append(p)
        event_counts.append(dips)
        avg_dip_mags.append(np.mean(dip_mags) if dip_mags else 0.0)

meters = np.array(meters)
print("Generated meter signals shape:", meters)
labels = np.array(labels)
event_counts = np.array(event_counts)
avg_dip_mags = np.array(avg_dip_mags)

# Store metadata in MongoDB
metadata = []
for i in range(n_meters):
    doc = {
        "Meter_ID": f"MTR{str(i+1).zfill(4)}",
        "True_Phase": int(labels[i]),
        "Event_Count": int(event_counts[i]),
        "Avg_Dip_Magnitude": float(meters[i]),
        "Timestamp": pd.Timestamp.now()
    }
    metadata.append(doc)

print(f"Inserting {len(metadata)} meter metadata documents into MongoDB...")
print(metadata[0])  # print first document as sample


#db["meters"].insert_many(metadata)

# Save a CSV of raw meter traces (wide format)
# raw_df = pd.DataFrame(meters, columns=[f"t_{i}" for i in range(intervals)])
# raw_df.insert(0, "Meter_ID", [f"MTR{str(i+1).zfill(4)}" for i in range(n_meters)])
# raw_df.insert(1, "True_Phase", labels)
# raw_df.to_csv('/content/drive/MyDrive/SAS Hackathon 2025/ami_synthetic_raw_traces.csv', index=False)

# print('Generated synthetic AMI data:')
# print(' - meters:', meters.shape)
# print(' - saved raw traces to /content/drive/MyDrive/SAS Hackathon 2025/ami_synthetic_raw_traces.csv')

# fig, ax = plt.subplots(figsize=(10,4))
# sample_indices = [0, n_meters_per_phase, 2*n_meters_per_phase]
# for idx in sample_indices:
#     ax.plot(t, meters[idx], label=f"Meter {idx} TruePhase={labels[idx]}")
# ax.plot(t, phase_refs[0], linestyle='--', linewidth=1, label='PhaseRef0 (example)')
# ax.set_xlabel('15-min Interval Index (0..95)')
# ax.set_ylabel('Voltage (V)')
# ax.set_title('Example Meter Voltage Traces (one meter per phase)')
# ax.legend()
# plt.show()
