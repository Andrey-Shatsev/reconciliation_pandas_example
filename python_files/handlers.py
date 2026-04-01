import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Optional

# --- Вспомогательный форматер (SRP: Single Responsibility Principle) ---
class NameFormatter:
    @staticmethod
    def to_short_fio(fullname: str) -> str:
        """Превращает 'Иванов Иван Иванович' в 'Иванов И. И.'"""
        if pd.isna(fullname) or not str(fullname).strip():
            return fullname
        parts = str(fullname).split()
        if len(parts) == 1:
            return parts[0]
        surname = parts[0]
        initials = [f"{p[0]}." for p in parts[1:3]]
        return f"{surname} {' '.join(initials)}"
    
# --- Абстрактный базовый класс ---
class BaseExcelProcessor(ABC):
    
    def __init__(self, file_path: str):
            self.file_path = file_path
            self.col_names = {
                0: 'raw_name', 
                1: 'department', 
                2: 'position', 
                4: 'planned_fot', 
                6: 'actual_amount'
            }
    
    def run(self) -> pd.DataFrame:
            """Основной метод оркестрации (Template Method)"""
            excel_data = pd.ExcelFile(self.file_path)
            processed_chunks = []

            for sheet_name in excel_data.sheet_names:
                
                # Вызываем специфичную логику обработки (реализуется в наследниках)
                df_processed = self.process_sheet(sheet_name)
                
                if df_processed is not None:
                    processed_chunks.append(df_processed)

            # Собираем всё в одну таблицу
            if not processed_chunks:
                return pd.DataFrame()
                
            return pd.concat(processed_chunks, ignore_index=True)

    @abstractmethod
    def process_sheet(self, sheet_name: str) -> Optional[pd.DataFrame]:
        """Этот метод должен переопределить каждый наследник"""
        pass
    

# --- Конкретный класс 1: Обработка ЗУП (наш случай с иерархией) ---
class ZupReportProcessor(BaseExcelProcessor):
    def process_sheet(self, sheet_name: str) -> pd.DataFrame:
        # Читаем сырые данные
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, skiprows=2, header=None)
        
        # Предварительная базовая очистка (переименование)
        df = df.rename(columns=self.col_names)
        
        # 1. Определяем маску сотрудника
        is_employee_row = df['department'].notna()

        # 2. Протягиваем данные вниз (ffill)
        df['Сотрудник'] = df['raw_name'].where(is_employee_row)
        df['Подразделение_фикс'] = df['department']
        df['Должность_фикс'] = df['position']
        
        cols_to_fill = ['Сотрудник', 'Подразделение_фикс', 'Должность_фикс']
        df[cols_to_fill] = df[cols_to_fill].ffill()

        # 3. Форматируем имя (используем наш вспомогательный класс)
        df['Сотрудник сокр'] = df['Сотрудник'].apply(NameFormatter.to_short_fio)

        # 4. Определяем вид начисления и фильтруем
        df['Вид_начисления'] = df['raw_name'].where(~is_employee_row)
        df['Период'] = sheet_name
        df['Период'] = pd.to_datetime(df['Период'], format='%m.%Y')

        # Оставляем только строки с начислениями
        df_flat = df[~is_employee_row & df['Вид_начисления'].notna()].copy()
        
        df_res = df_flat[['Период', 'Сотрудник сокр', 'Подразделение_фикс', 'Должность_фикс', 'Вид_начисления', 'actual_amount']].copy()
        df_res.rename(columns={'Подразделение_фикс':'Подразделение', 'Должность_фикс': "Должность", 'actual_amount':"Начислено"}, inplace=True)   
        df_res.dropna(inplace=True)
        
        return df_res


# --- Конкретный класс 2: для ведомости Т-51 ---
class T51(BaseExcelProcessor):
    def __init__(self, file_path: str):
        # 1. Сначала вызываем конструктор родителя
        super().__init__(file_path)
        
        self.col_names = {
            'Unnamed: 2':"Сотрудник сокр",
            'Unnamed: 3':"Должность"   
            }
        
        self.drop_col = [
                    "Unnamed: 0", "Unnamed: 1", "Unnamed: 4", "Unnamed: 5",
                    "Unnamed: 7", "Unnamed: 8", "рабочих", "выход-\nных и празд-\nничных",
                    "Unnamed: 10", "Unnamed: 11"
                ]
        
    def _find_stop_column(self, columns):
        for i, col in enumerate(columns):
            # Превращаем заголовок в одну строку (даже если это кортеж)
            col_name = str(col).lower()
            if "всего" in col_name:
                return i
        return None    
            

    def process_sheet(self, sheet_name: str) -> pd.DataFrame:
        
        df = pd.read_excel('Данные Т51.xlsx', sheet_name=sheet_name, skiprows=2) 
        df.rename(columns=self.col_names, inplace=True)
        
        stop_idx = self._find_stop_column(df.columns)
        if stop_idx is not None:
            df = df.iloc[:, :stop_idx]
        
        df.drop(columns=self.drop_col, inplace= True)
        df = df.dropna(subset=['Сотрудник сокр'])
        df = df[df['Сотрудник сокр'] != "3"] 
        
        # Список столбцов, которые мы оставляем (измерения)
        id_vars = ['Сотрудник сокр', 'Должность']

        # Делаем "расплавление" таблицы
        df_melted = df.melt(
            id_vars=id_vars, 
            var_name='Вид_начисления', 
            value_name='Начислено'
        )
        df_melted = df_melted[df_melted['Начислено'].notna() & (df_melted['Начислено'] != 0)]
        
        df_melted['Период'] = sheet_name
        df_melted['Период'] = pd.to_datetime(df_melted['Период'], format='%m.%Y')
        
        return df_melted


# --- Пример использования ---
if __name__ == "__main__":
    path = 'Данные Т51.xlsx'
    
    # Мы можем легко переключаться между процессорами
    processor = T51(path)
    final_df = processor.run()
    
    print(final_df.info())
    print(final_df.head())