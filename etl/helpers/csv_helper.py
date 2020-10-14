import io
import pandas as pd


class CsvHelper:
    def __init__(self):
        pass

    def get_csv_from_url(self, url):
        csv = pd.read_csv(url)
        return csv
    
    def filter_by_column_value(self, csv, column, value):
        has_value = csv[column] == value
        filtered_csv = csv[has_value]
        return filtered_csv
    
    def format_date(self, csv, date_field):
        csv[date_field] =  pd.to_datetime(csv[date_field])
        return csv
    
    def purge(self, csv, csv_field, entries_to_purge, entry_to_purge_field):
        for entry in entries_to_purge:
            csv = csv[csv[csv_field] != entry[entry_to_purge_field]]
        return csv