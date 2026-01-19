import pandas as pd

from base_transformer import BaseTransformer

class DataNormalizer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Нормализация строк
        df['country'] = df['country'].str.upper().str.strip()
        df['city'] = df['city'].str.title().str.strip()
        df['email'] = df['email'].str.lower().str.strip()
        
        # Нормализация телефонов (убрать все кроме цифр)
        df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)
        
        # Стандартизация дат
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['order_date'] = df['order_date'].dt.tz_localize(None)
        
        # Округление цен
        df['total_amount'] = df['total_amount'].round(2)
        
        return df