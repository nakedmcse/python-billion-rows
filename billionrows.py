# Python implementation of the billion rows challenge
import argparse
from itertools import islice
from typing import Any, Generator
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
from multiprocessing.spawn import freeze_support


# Generate 1 billion rows of random data
def generate_chunk(stations: list, num_rows: int, seed: float, filename: str) -> None:
    np.random.seed(seed)  # Important: unique seed per process
    station_indices = np.random.randint(0, len(stations), size=num_rows)
    values = np.random.uniform(-100, 100, size=num_rows)

    chunk = [f"{stations[i]};{values[j]:.2f}" for j, i in enumerate(station_indices)]
    with open(filename, 'ab', buffering = 10240*1024) as f:
        for line in chunk:
            f.write(line.encode('utf-8') + b'\n')

def generate(filename: str) -> None:
    print('Reading weather station names')
    with open('weather_stations.csv') as file:
        stations = [n[0] for n in (x.split(';') for x in file.read().splitlines() if not "#" in x)]

    chunk_size = 10_000_000
    total = 1_000_000_000
    num_chunks = total // chunk_size
    num_procs = 14

    print(f'Generating {total:,} rows using {num_procs} workers...')

    with ProcessPoolExecutor(max_workers=num_procs) as executor:
        futures = [executor.submit(generate_chunk, stations, chunk_size, seed, filename) for seed in range(num_chunks)]
    for future in as_completed(futures):
        print('Chunk complete')

# Parse given data file to {station=min/avg/max}
def read_file_chunk(filename: str, lines: int =1000) -> Generator[list[str], Any, None]:
    with open(filename, 'r') as file:
        while True:
            chunk = list(islice(file, lines))
            if not chunk:
                break
            yield chunk

def parse_chunk(chunk: list[str]) -> {}:
    output_values = {}
    for row in chunk:
        try:
            key, val = row.split(';')
            val = float(val)
        except (ValueError, IndexError):
            continue

        if key not in output_values:
            output_values[key] = [1, val, val, val]  # count, max, sum, min
        else:
            stats = output_values[key]
            stats[0] += 1
            stats[1] = max(stats[1], val)
            stats[2] += val  # sum
            stats[3] = min(stats[3], val)

    for key, stats in output_values.items():
        stats[2] = stats[2] / stats[0]  # replace sum with average
    return output_values

def parse(filename: str) -> None:
    chunk_size = 10_000_000
    num_procs = 8

    output_values = {}
    with ProcessPoolExecutor(max_workers=num_procs) as executor:
        futures = [executor.submit(parse_chunk, chunk) for chunk in read_file_chunk(filename, chunk_size)]
    for future in as_completed(futures):
        data = future.result()
        for k, v in data.items():
            if k not in output_values:
                output_values[k] = v
            else:
                existing_vals = output_values[k]
                existing_vals[0] += v[0]
                existing_vals[1] = max(existing_vals[1], v[1])
                existing_vals[2] += v[2]
                existing_vals[3] = min(existing_vals[3], v[3])
                output_values[k] = existing_vals

    for k, v in output_values.items():
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
