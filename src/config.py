RAW_DATA_DIR = 'data/raw'
PROCESSED_DATA_DIR = 'data/processed'
EXPECTED_RANGES = {
    'temperature': (-10, 60),
    'humidity': (0, 100),
    'soil_moisture': (0, 100),
    'light_intensity': (0, 2000),
    'battery': (2.5, 4.2)
}
CALIBRATION = {
    'temperature': (1.02, -0.5),
    'humidity': (0.98, 1.0),
    'soil_moisture': (1.0, 0.0),
    'light_intensity': (1.0, 0.0),
    'battery': (1.0, 0.0)
}
