import os
from dotenv import load_dotenv
import gspread
import pandas as pd
from logger import Logger

load_dotenv()


class GoogleSpreadsheet:
    def __init__(self, url_env_name):
        # Authorize the client
        self.gc = gspread.service_account(os.getenv('PATH_OF_SERVICE_ACCOUNT_FILE'))
        # Initialize logger
        self.logger = Logger('spreadsheet.log')
        self.url_env_name = os.getenv(url_env_name)

    def get_spreadsheet(self):
        # Open a sheet from a spreadsheet
        try:
            return self.gc.open_by_url(self.url_env_name)
        except Exception as e:
            self.logger.log(f"Failed to get spreadsheet : {str(e)}")
            return None

    def get_worksheet(self, name):
        # Open a worksheet from a spreadsheet
        try:
            return self.get_spreadsheet().worksheet(name)
        except Exception as e:
            self.logger.log(f"Failed to get worksheet {name}: {str(e)}")
            return None
    
    def get_worksheet_data(self, name):
        # Get all values from the worksheet
        worksheet = self.get_worksheet(name)
        values = worksheet.get_all_values()
        return pd.DataFrame(values[1:], columns=values[0])
    
    def add_dataframe_to_worksheet(self, df, worksheet_name, clear_worksheet=False):
        # Add a dataframe to a worksheet
        worksheet = self.get_worksheet(worksheet_name)
        if clear_worksheet:
            worksheet.clear()
            worksheet.append_row(df.columns.tolist())
        data = df.values.tolist()
        worksheet.append_rows(data)

    def update_worksheet(self, worksheet_name, data_dict):
        # Update a worksheet, for work control
        worksheet = self.get_worksheet(worksheet_name)
        
        worksheet.append_row([data_dict.get(column_name, '') for column_name in worksheet.get_all_values()[0]])
        # for i, column_name in enumerate(worksheet.get_all_values()[0]):
        #     cell_value = worksheet.cell(1, i+1).value
        #     if cell_value.isdigit():
        #         worksheet.update_cell(1, i+1, int(cell_value) + data_dict[column_name])