"""
serializer.py
=============

"""

# Standard library
import io
from typing import Any

# Third-party
import pandas as pd


class Serializer:    
    LOCALES: dict[str, dict[str, str]] = {
        'RFC4180': {
            'sep': ',',
            'decimal': '.'
        },
        'DE': {
            'sep': ';',
            'decimal': ','
        }
    }

    def __init__(self):
        pass

    @classmethod
    def df_to_csv(
        cls,
        df: pd.DataFrame,
        dtype_map: dict[str, str | type]={},
        places_map: dict[str, int]={},
        na_map: dict[str, Any]={},
        include_index: bool=False,
        locale: str='RFC4180'
        ) -> io.BytesIO:
        df = df.astype(dtype_map)

        col_map = {
            col: {
                'places': None,
                'na': ''
            } for col in df.columns
        }

        for col, v in places_map.items():
            col_map[col]['places'] = v

        for col, v in na_map.items():
            col_map[col]['na'] = v

        for col in df.select_dtypes(include='float').columns:
            df[col] = df[col].apply(
                cls.float_to_str,
                places=col_map[col]['places'],
                na=col_map[col]['na'],
                locale=locale
            )

        for col in na_map.keys():
            df[col] = df[col].fillna(na_map[col]['na'])

        stream: io.BytesIO = io.BytesIO()

        df.to_csv(
            stream,
            sep=';',
            decimal=',',
            index=False,
            na_rep='',
            encoding='utf-8',
            lineterminator='\n'
        )

        return stream

    @classmethod
    def float_to_str(cls, value: float, places: int | None, na: str, locale: str) -> str:
        decimal: str = cls.LOCALES[locale]['decimal']

        if places is None:
            s: str = na if pd.isna(value) else f'{value}'
        else:
            s: str = na if pd.isna(value) else f'{value:.{places}f}'

        return s.replace('.', decimal)
