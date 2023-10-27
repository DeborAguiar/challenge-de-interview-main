import pandas as pd
import re


class EquipmentFailureProcessor:
    
    def process_data(self, data_folder):
        """
        Processa os dados e imprime os resultados na saída padrão.
        """

        self._extract_data(data_folder)
        
        total_failure_count = self._total_failure_count(self.df_logs)
        most_failed_equipment_name = self._most_failed_equipment_name(self.df_logs, self.df_equipment_sensors, self.df_equipment)
        groups_mean = self._group_mean_failures(self.df_equipment)
        most_failed_sensor_per_equipment = self._most_failed_sensor_per_equipment()

        print(f"Total de falhas: {total_failure_count}")
        print(f"Equipamento com mais falhas: {most_failed_equipment_name}")
        print(f"Média por grupo:\n{groups_mean.to_string(header=False)}")
        print(f"Sensor que mais deu erro para cada equipamento:\n{most_failed_sensor_per_equipment}")

    def _extract_data(self, data_folder):
        """
        Carrega e extrai os dados iniciais do arquivo CSV, JSON e TXT fornecidos.
        """
        self.df_equipment_sensors = pd.read_csv(f"{data_folder}/equipment_sensors.csv")
        self.df_equipment = pd.read_json(f"{data_folder}/equipment.json")
        with open(f"{data_folder}/equipment_failure_sensors.txt", "r") as file:
            lines = file.readlines()
            for line in lines:
                sensors_logs = {
                    "log_types": [],
                    "sensor_id": []
                }
                match = re.match(r'\[(.*?)\]\s+(.*?)\s+sensor\[(\d+)\]:', line)
                if match:
                    log_types, sensor_id = match.groups()[1], match.groups()[2]
                    sensors_logs["log_types"].append(log_types)
                    sensors_logs["sensor_id"].append(int(sensor_id))
        self.df_logs = pd.DataFrame(sensors_logs)

    def _total_failure_count(self, df_logs):
        """
        Calcula o número total de falhas de equipamento que ocorreram.

        Returns:
        int: O número total de falhas de equipamento.
        """
        return len(df_logs[df_logs["log_types"] == "ERROR"])

    def _most_failed_equipment_name(self, df_logs, df_equipment_sensors, df_equipment):
        """
        Identifica o nome do equipamento que teve mais falhas.

        Returns:
        str: O nome do equipamento com mais falhas.
        """
        sensorId_equipmentId = pd.merge(df_logs['sensor_id'], df_equipment_sensors)
        self.count_equipment_byGroup = sensorId_equipmentId['equipment_id'].value_counts()
        most_failed_equipment_id = self.count_equipment_byGroup.idxmax()
        return df_equipment[df_equipment['equipment_id'] == most_failed_equipment_id]['name'].values[0]

    def _group_mean_failures(self, df_equipment):
        """
        Calcula a quantidade média de falhas por grupo de equipamentos, ordenada por número de falhas em ordem crescente.

        Returns:
        pandas.Series: A série contendo a média por grupo de equipamentos.
        """
        
        groupName_equipmentId_count = pd.DataFrame({
            'group_name': df_equipment['group_name'],
            'id do equipamento': self.count_equipment_byGroup.index,
            'count_failed_equipment': self.count_equipment_byGroup.values
        })
        groups_mean = groupName_equipmentId_count.groupby('group_name')['count_failed_equipment'].mean()
        return groups_mean.sort_values()


    def _most_failed_sensor_per_equipment(self):
        """
        Encontra os sensores que apresentam o maior número de erros por nome de equipamento em um grupo de equipamentos.

        Returns:
        pandas.DataFrame: O DataFrame contendo os sensores que mais deram erro para cada equipamento.
        """
        sensorId_equipmentId = pd.merge(self.df_logs['sensor_id'], self.df_equipment_sensors)
        most_failed_sensor_per_equipment = sensorId_equipmentId.groupby('equipment_id')['sensor_id'].agg(lambda x: x.value_counts().idxmax())
        sensor_errors_count = sensorId_equipmentId['sensor_id'].value_counts()
        most_failed_sensor_per_equipment = pd.DataFrame(most_failed_sensor_per_equipment)
        most_failed_sensor_per_equipment['error_count'] = most_failed_sensor_per_equipment['sensor_id'].map(sensor_errors_count)
        final_result = pd.merge(most_failed_sensor_per_equipment, self.df_equipment, on='equipment_id', how='left')[['sensor_id', 'error_count', 'name', 'group_name']]
        return final_result.sort_values(by='error_count', ascending=False)

processor = EquipmentFailureProcessor()
processor.process_data("data")
