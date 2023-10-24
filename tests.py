import pandas as pd
import unittest
from routine import EquipmentFailureProcessor

class Test_TotalFailureCount(unittest.TestCase):

    def test_loads_data_successfully(self):
        # Arrange
        processor = EquipmentFailureProcessor()
        
        processor._extract_data("data")
    
        # Assert
        self.assertIsNotNone(processor.df_equipment_sensors)
        self.assertIsNotNone(processor.df_equipment)
        self.assertIsNotNone(processor.df_logs)

    def test_errors_number_in_log(self):
        processor = EquipmentFailureProcessor()
        df_logs = pd.DataFrame({
            "log_types": ["ERROR", "ERROR", "ERROR", "WARNING"],
            "sensor_id": [1, 2, 3, 4]
        })
        result = processor._total_failure_count(df_logs)
        self.assertEqual(result, 3)
    
    def test_most_failed_equipment_name(self):
        processor = EquipmentFailureProcessor()
        
        processor.df_logs = pd.DataFrame({
            "log_types": ["ERROR", "ERROR", "ERROR"],
            "sensor_id": [1, 2, 3]
        })
        processor.df_equipment_sensors = pd.DataFrame({
            "sensor_id": [1, 2, 3],
            "equipment_id": [1, 1, 2]
        })
        processor.df_equipment = pd.DataFrame({
            "equipment_id": [1, 2],
            "name": ["Equipment A", "Equipment B"]
        })

        result = processor._most_failed_equipment_name(processor.df_logs, processor.df_equipment_sensors, processor.df_equipment)

        self.assertEqual(result, "Equipment A")

    def test_calculate_mean_count(self):
        processor = EquipmentFailureProcessor()
        
        """
        1 grupo = 3 equipamentos = 6 sensores = 12 erros
                  1 equipamento  = 2 sensores =  4 erros
                                 = 1 sensor   =  2 erro 
        """

        processor.df_logs = pd.DataFrame({
            'log_types': ["ERROR"] * 36,
            'sensor_id': list(range(1, 19))*2
        })
        processor.df_equipment_sensors = pd.DataFrame({
            "sensor_id": list(range(1, 19)),
            "equipment_id": list(range(1, 10))*2
        })
        processor.df_equipment = pd.DataFrame({
            "equipment_id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "name": [f'eq{i}' for i in range(1, 10)],
            "group_name": ['A', 'A', 'A', 'B', 'B', 'B', 'C', 'C', 'C']
        })
        
        processor.sensorId_equipmentId = pd.merge(processor.df_logs['sensor_id'], processor.df_equipment_sensors)
        processor.count_equipment_byGroup = processor.sensorId_equipmentId['equipment_id'].value_counts()
        
        result = processor._group_mean_failures(processor.df_equipment)
        
        self.assertEqual(result.iloc[0], 4)        


if __name__ == '__main__':
    unittest.main()