from external_libs import datetime

current_date = datetime.now().strftime("%d%m%Y_%H%M%S")

RAW_DATA_PATH = 'c:/Users/carol/Documents/CCH/0_world_alcohol_consumption_dataset/data/raw/raw.csv'
CLEAN_DATA_PATH = 'c:/Users/carol/Documents/CCH/0_world_alcohol_consumption_dataset/data/clean/clean.csv'
CLEAN_ADJ_DATA_PATH = 'c:/Users/carol/Documents/CCH/0_world_alcohol_consumption_dataset/data/clean/clean_adjusted.csv'
GRAPHICS_PATH = 'c:/Users/carol/Documents/CCH/0_world_alcohol_consumption_dataset/graph/'
LOGS_PATH = f'c:/Users/carol/Documents/CCH/0_world_alcohol_consumption_dataset/logs/0_world_alcohol_consumption_dataset{current_date}.txt'
