import sys
import unittest
import pandas as pd

sys.path.append('etl')
from helpers.env_helper import EnvHelper
from data_accessors.covid_19_data_accessor import Covid19DataAccessor

class TestCsvHelper(unittest.TestCase):

    def test_valid_jh_dataset(self):
        env_helper = EnvHelper()
        covid_19_data_accessor = Covid19DataAccessor(env_helper)

        mock_jh = df1=pd.DataFrame({
            'Recovered':[1,2,3,4,5],
            'Date':['2010-01-01','2010-01-02','2010-01-03','2010-01-04','2010-01-04']
        })

        response = covid_19_data_accessor.validate_jh(mock_jh)
        expected = True
        assert response == expected

    def test_invalid_jh_dataset(self):
        env_helper = EnvHelper()
        covid_19_data_accessor = Covid19DataAccessor(env_helper)

        mock_jh = df1=pd.DataFrame({
            'Not_Recovered':[1,2,3,4,5],
            'Not_Date':['2010-01-01','2010-01-02','2010-01-03','2010-01-04','2010-01-04']
        })

        response = covid_19_data_accessor.validate_jh(mock_jh)
        expected = False
        assert response == expected

    def test_valid_nyt_dataset(self):
        env_helper = EnvHelper()
        covid_19_data_accessor = Covid19DataAccessor(env_helper)

        mock_nyt = df1=pd.DataFrame({
            'deaths':[1,2,3,4,5],
            'cases':[1,2,3,4,5],
            'date':['2010-01-01','2010-01-02','2010-01-03','2010-01-04','2010-01-04']
        })

        response = covid_19_data_accessor.validate_nyt(mock_nyt)
        expected = True
        assert response == expected

    def test_invalid_nyt_dataset(self):
        env_helper = EnvHelper()
        covid_19_data_accessor = Covid19DataAccessor(env_helper)

        mock_nyt = df1=pd.DataFrame({
            'not_deaths':[1,2,3,4,5],
            'not_cases':[1,2,3,4,5],
            'not_date':['2010-01-01','2010-01-02','2010-01-03','2010-01-04','2010-01-04']
        })

        response = covid_19_data_accessor.validate_nyt(mock_nyt)
        expected = False
        assert response == expected

if __name__ == "__main__":
    unittest.main()