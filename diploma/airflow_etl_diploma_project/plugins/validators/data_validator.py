import pandas as pd

class DataValidator:
    def validate_orders(self, df: pd.DataFrame) -> tuple:
        """Возвращает (valid_df, invalid_df, errors)"""
        errors = []
        
        # Проверка обязательных полей
        required_fields = ['order_id', 'customer_id', 'total_amount']
        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Missing required field: {field}")
        
        # Проверка типов
        if not pd.api.types.is_numeric_dtype(df['total_amount']):
            errors.append("total_amount must be numeric")
        
        # Проверка диапазонов
        invalid_amount = df[
            (df['total_amount'] < 0) | (df['total_amount'] > 1000000)
        ]
        if len(invalid_amount) > 0:
            errors.append(f"Found {len(invalid_amount)} records with invalid amount")
        
        # Проверка статусов
        valid_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        invalid_status = df[~df['status'].isin(valid_statuses)]
        
        # Разделение на валидные и невалидные
        valid_mask = (
            df['total_amount'].between(0, 1000000) &
            df['status'].isin(valid_statuses)
        )
        
        valid_df = df[valid_mask]
        invalid_df = df[~valid_mask]
        
        return valid_df, invalid_df, errors