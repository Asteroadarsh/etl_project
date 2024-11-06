import unittest
import os
import pandas as pd
from main import read_csv, clean_and_convert_date, transform_data, load_data

class TestETLFunctions(unittest.TestCase):

    def test_read_csv(self):
        file_path = os.getenv('FILE_PATH', 'employee_details.csv')
        df = read_csv(file_path)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)

    def test_clean_and_convert_date(self):
        self.assertEqual(clean_and_convert_date('2022-01-01'), '01/01/2022')
        self.assertEqual(clean_and_convert_date('01-01-2022'), '01/01/2022')
        
        self.assertIsNone(clean_and_convert_date('invalid-date'))
        self.assertIsNone(clean_and_convert_date('2022/01/01'))

    def test_transform_data(self):
        data = {
            'FirstName': ['John', 'Jane'],
            'LastName': ['Doe', 'Smith'],
            'BirthDate': ['1990-01-01', '1985-05-15'],
            'Salary': [45000, 75000]
        }
        df = pd.DataFrame(data)

        transformed_df = transform_data(df)

        self.assertIn('FullName', transformed_df.columns)
        self.assertIn('Age', transformed_df.columns)
        self.assertIn('SalaryBucket', transformed_df.columns)
        self.assertEqual(transformed_df['FullName'][0], 'John Doe')
        self.assertEqual(transformed_df['Age'][0], 33)  
        self.assertEqual(transformed_df['SalaryBucket'][0], 'A')

    def test_load_data(self):
        data = {
            'FullName': ['John Doe', 'Jane Smith'],
            'Age': [33, 37],
            'SalaryBucket': ['A', 'B']
        }
        df = pd.DataFrame(data)
        
        db_url = 'postgresql://adarsh:zxcvbnm@db:5432/etl_db'  
        table_name = 'test_employee_data'
        
        try:
            load_data(df, db_url, table_name)
            self.assertTrue(True)  
        except Exception as e:
            self.fail(f"Loading data failed: {e}")


if __name__ == '__main__':
    unittest.main()
