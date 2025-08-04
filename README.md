# Billion Row Challenge - Python Implementation

A high-performance Python implementation of the [Billion Row Challenge](https://github.com/gunnarmorling/1brc) that processes one billion temperature measurements as fast as possible.

Performance enhancements take inspiration from [VictoriqueMoe Go Version](https://github.com/VictoriqueMoe/go-billion-rows).
## Overview

This implementation processes a text file containing one billion rows of weather station temperature data in the format:
```
Station Name;Temperature
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

## Features

- **Multi processing** using all available CPU cores
- **Data generator** to create test files with realistic weather station data

## Usage

### Generate Test Data

First, you'll need a `weather_stations.csv` file containing weather station names (one per line or semicolon-separated). Then generate the billion-row dataset:

```bash
python3 data.csv --generate
```

This creates a `data.csv` file with 1 billion temperature measurements (~16 GB).

### Process the Data

Run the challenge processor:

```bash
python3 data.csv --parse
```

The program will output results in the format:
```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, ...}
```

## Performance Optimizations

- **Parallel processing**: Work is distributed across all CPU cores
- **based processing**: File is split into worker-sized chunks at line boundaries

