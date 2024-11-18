from external_libs import logging # import libraries 
from libs.logger import * # save logs and level
from config import * # import config



from libs.data_loader import DataLoader
from libs.statistics_analyzer import *
from libs.columns_workers import *
from libs.data_saver import DataSaver
from libs.data_contener import country_population


class MainRunner:
    def __init__(self):
        self.logger = logging.getLogger(__name__)


        # IMPORT DATA AND FIRST ANALYSIS REPORT 
        raw = DataLoader(RAW_DATA_PATH).get_df() # load data 
        self.logger.info(f"------ Import Data and 1st Analysis Report {RAW_DATA_PATH} ------")
        basic_report(['Count'], raw) # vérifier  median classic et lib


        # DATA PROCESSING 
        self.logger.info("------ Data Processing ------")
        clean = pd.DataFrame()
        
        # rename columns 
        clean = columns_renamer([
            'Gender', 'gender',
            'Count', 'count',
            'Countries', 'countries',
            'Countries Code',
            'countries_code',
            'Date', 'date',
            ],raw) 
        
        # create namibia country code 
        logger.info("adding unique code for Namibia country")
        clean.loc[clean['countries'] == 'namibia', 'countries_code'] = 'nam'

        # text to code
        clean = columns_text2code(
            ['gender', 'countries_code'],
            clean)

        # date conversion 
        clean = date_converter(
            clean,
            ['date'],
            expected_format='%m/%d/%Y')

        # SECOND ANALYSIS REPORT AND SAVE CLEANED DATA           
        self.logger.info(f"------ Second Analysis Report {CLEAN_DATA_PATH} ------")
        basic_report(['count', 'gender', 'countries_code'], clean)
        DataSaver(clean, CLEAN_DATA_PATH, 'csv').save_data_in_batches()
        print(f"\nclean:\n {clean}")
        print(clean)



        # PROCESS AND SAVE DATA FOR POWERBI ANALYSIS AND REPORTS 
        # create second cleaned file with consumption ajdusted ['countries', 'pop', 'count', 'ratio']
        cleanadj = pd.DataFrame(clean)
        cleanadj = cleanadj.groupby("countries").agg({
            "count": "sum",
            "countries_code": "first"  # prend le premier code pour chaque groupe
        }).reset_index()
        cleanadj["pop"] = cleanadj["countries"].map(country_population)
        cleanadj = cleanadj[cleanadj['pop'] >= 100000] # delete high bias data 
        cleanadj["ratio"] = ((cleanadj["count"] / cleanadj["pop"]) * 10000).round(2)

        # SECOND ANALYSIS REPORT AND SAVE CLEANED DATA           
        self.logger.info(f"------ Second Analysis Report {CLEAN_ADJ_DATA_PATH} ------")
        basic_report(['count', 'pop', 'ratio'], cleanadj)
        DataSaver(cleanadj, CLEAN_ADJ_DATA_PATH, 'csv').save_data_in_batches()
        print(f"\nclean_adjusted:\n {cleanadj}")

MainRunner()


