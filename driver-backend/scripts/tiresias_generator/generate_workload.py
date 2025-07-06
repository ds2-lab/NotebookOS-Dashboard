import random
import pandas as pd

from tqdm import tqdm
from datetime import datetime

import warnings
warnings.filterwarnings(
    action='ignore', category=UserWarning, message=r"Boolean Series.*"
)

import time

model_names = ['vgg19', 'vgg16', 'vgg11', 'alexnet', 'resnet152', 'resnet101', 'resnet50', 'inception4', 'inception3']

def process_df(df, start_ts: int = 0, end_ts: int = 1630468800):
    start_time = time.time()

    # Precompute timestamps and cache results
    df['start_timestamp'] = df['start_time'].map(lambda x: datetime.fromisoformat(x).timestamp())
    df['stop_timestamp'] = df['stop_time'].map(lambda x: datetime.fromisoformat(x).timestamp())

    print("start_ts: ", start_ts)
    print("end_ts: ", end_ts)

    df = df[df['start_timestamp'] >= start_ts]
    df = df[df['stop_timestamp'] < end_ts]

    print(df)

    jobs = []

    lowest_timestamp = df['start_timestamp'].min()

    # Group data by session ID
    grouped = df.groupby('session')

    job_id = 0

    for session_id, df_session in tqdm(grouped):
        # Calculate start and stop ticks
        trainings_df = df_session[df_session['name'] == "TrainingEnded"]

        model: str = random.choice(model_names)

        for _, training in trainings_df.iterrows():
            t_start = int((training['start_timestamp'] - lowest_timestamp) // 60)
            t_end = int((training['stop_timestamp'] - lowest_timestamp) // 60)

            if t_end <= t_start:
                continue

            t_dur = t_end - t_start

            if t_dur <= 0:
                continue

            # With relatively low probability, switch models.
            if random.random() < 0.15:
                model: str = random.choice(model_names)

            # job_id,num_gpu,submit_time,iterations,model_name,duration,interval
            jobs.append({
                "job_id": job_id,
                "num_gpu": max(training['max_num_gpus'], 1),
                "submit_time": t_start,
                "iterations": 0,
                "model_name": model,
                "duration": t_dur,
                "interval": 0,
            })

            job_id += 1

    print("Time elapsed: %.2f seconds." % (time.time() - start_time))

    job_df = pd.DataFrame(jobs, columns=['job_id', 'num_gpu', 'submit_time', 'iterations', 'model_name', 'duration', 'interval'])

    job_df.to_csv("sensei_june_60sec.csv", header=True, index=False)

jan_1_2022_ts = 1641013200
may_31_ts = 1622433600
june_1_ts = 1622520000
july_1_ts = 1625112000
august_1_ts = 1627790400
sept_1_ts = 1630468800

# input_df = pd.read_csv(r"C:\Users\benrc\go\src\github.com\sensei-storage-optimus\output\Preprocess 15sec June\summary_events.csv")
input_df = pd.read_csv(r"C:\Users\benrc\go\src\github.com\sensei-storage-optimus\output\Preprocess 60sec Jun-Aug\summary_events.csv")
process_df(input_df, june_1_ts, july_1_ts)