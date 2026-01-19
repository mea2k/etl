
import pandas as pd

class DataQuality:
	"""Класс для оценки качества данных."""
	def assess_data_quality(self,df: pd.DataFrame) -> dict:
			"""Оценка качества данных."""
			
			total_records = len(df)
			
			quality_metrics = {
					'total_records': total_records,
					
					# Completeness (полнота)
					'completeness': {
							field: (df[field].notna().sum() / total_records * 100)
							for field in df.columns
					},
					
					# Uniqueness (уникальность)
					'uniqueness': {
							'order_id': (df['order_id'].nunique() / total_records * 100),
							'customer_id': (df['customer_id'].nunique() / total_records * 100)
					},
					
					# Validity (валидность)
					'validity': {
							'valid_emails': (df['email'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$').sum() / total_records * 100),
							'valid_amounts': ((df['total_amount'] > 0).sum() / total_records * 100)
					},
					
					# Consistency (консистентность)
					'duplicates': df.duplicated(subset=['order_id']).sum(),
					'null_values': df.isnull().sum().to_dict()
			}
			
			return quality_metrics