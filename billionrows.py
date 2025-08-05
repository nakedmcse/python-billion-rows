# Python implementation of the billion rows challenge
import argparse
import os
import mmap
from mmap import ACCESS_READ
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from multiprocessing.spawn import freeze_support

from sympy.core.numbers import Infinity


# Generate 1 billion rows of random data
def generate_chunk(stations: list, num_rows: int, seed: float, filename: str) -> None:
    np.random.seed(seed)  # Important: unique seed per process
    station_indices = np.random.randint(0, len(stations), size=num_rows)
    values = np.random.uniform(-100, 100, size=num_rows).round(2)

    with open(filename, 'ab', buffering=10240*1024) as f:
        for j, i in enumerate(station_indices):
            f.write(f"{stations[i]};{values[j]}".encode('ascii') + b'\n')

def generate(filename: str) -> None:
    print('Reading weather station names')
    with open('weather_stations.csv', encoding='ascii', errors='ignore') as file:
        stations = [n[0] for n in (x.split(';') for x in file.read().splitlines() if not "#" in x)]

    chunk_size = 10_000_000
    total = 1_000_000_000
    num_chunks = total // chunk_size
    num_procs = os.cpu_count()

    print(f'Generating {total:,} rows using {num_procs} workers...')

    with ProcessPoolExecutor(max_workers=num_procs) as executor:
        futures = [executor.submit(generate_chunk, stations, chunk_size, seed, filename) for seed in range(num_chunks)]
    for _ in as_completed(futures):
        pass

# Parse given data file to {station=min/avg/max}
def parse_chunk_mmap(filename: str, offset: int, size: int) -> {}:
    output_values = {}

    with open(filename, 'rb') as file:
        with mmap.mmap(file.fileno(), 0, access=ACCESS_READ) as m:
            m.seek(offset)
            chunk = m.read(size)
            for row in chunk.decode('ascii', errors='ignore').splitlines():
                try:
                    key, val_str = row.split(';')
                    val = float(val_str)
                    stats = output_values[key]
                    stats[0] += 1
                    stats[1] = max(stats[1], val)
                    stats[2] += val  # sum
                    stats[3] = min(stats[3], val)
                except KeyError:
                    output_values[key] = [1,val,val,val]
                except Exception:
                    continue

    for vals in output_values.values():
        vals[2] = vals[2] / vals[0]  # replace sum with average
    return output_values

def parse(filename: str) -> None:
    chunk_size = 10_000_000
    total = 1000_000_000
    num_chunks = total // chunk_size
    file_size = os.path.getsize(filename)
    chunk_bytes = file_size // num_chunks
    num_procs = os.cpu_count()
    print(f'Parsing {total:,} rows using {num_procs} workers...')

    output_values = {}
    with ProcessPoolExecutor(max_workers=num_procs) as executor:
        futures = [executor.submit(parse_chunk_mmap, filename, x, chunk_bytes) for x in range(0, file_size, chunk_bytes)]
        for future in as_completed(futures):
            data = future.result()
            for k, v in data.items():
                try:
                    existing_vals = output_values[k]
                    existing_vals[0] += v[0]
                    existing_vals[1] = max(existing_vals[1], v[1])
                    existing_vals[2] += v[2]
                    existing_vals[3] = min(existing_vals[3], v[3])
                except KeyError:
                    output_values[k] = v

    for v in output_values.values():
        v[2] = v[2] / v[0]

    sorted_stations = sorted(output_values.keys())
    output_lines = []
    for station in sorted_stations:
        output_lines.append(f"{station}={output_values[station][1]:.2f}/{output_values[station][2]:.2f}/{output_values[station][3]:.2f}")
    print("{" + ", ".join(output_lines) + "}")

# Main program
if __name__ == '__main__':
    freeze_support()
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str, help='Data file name')
    parser.add_argument('--generate', action="store_true", help='Generate weather data file')
    parser.add_argument('--parse', action="store_true", help='Parse weather data file')
    args = parser.parse_args()

    if args.generate:
        generate(args.filename)
    else:
        parse(args.filename)
