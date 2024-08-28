#!/usr/bin/env python

from pathlib import Path
import os
import subprocess
import itk
import json

oai_data_root = '/mnt/cybertron/OAI'
os.chdir(oai_data_root)

# months
time_points = [0, 12, 18, 24, 30, 36, 48, 72, 96]
time_point_folders = ['OAIBaselineImages',] + [f'OAI{m}MonthImages' for m in time_points[1:]]
time_point_patients = { tp: set() for tp in time_points }

for time_point_index, time_point in enumerate(time_points[:]):
    folder = Path(time_point_folders[time_point_index]) / 'results'
    for study in folder.iterdir():
        if study.is_dir():
            for patient in study.iterdir():
                if patient.is_dir() and str(patient.name).startswith('9'):
                    time_point_patients[time_point].add(patient.name)
            print(study)
# print(time_point_patients)
full_patients = time_point_patients[0].copy()
# 4796
print(len(full_patients))
# Most patients not in time point 30?
for time_point in [0, 12, 18, 24, 36, 48, 72, 96]:
    full_patients.intersection_update(time_point_patients[time_point])
    print('time point', time_point, len(full_patients))

full_patients = list(full_patients)
full_patients.sort()
# 181
print('full_patients:', full_patients)
print(len(full_patients))

with open('patients_with_8_time_points.json', 'w') as fp:
    json.dump(full_patients, fp)
