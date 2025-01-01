import pandas as pd 
import numpy as np
from abc import abstractmethod, ABC

class DataCleaningStrategy(ABC):
    @abstractmethod
    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method to clean data in a DataFrame.

        Parameters:
        data (pd.DataFrame): A DataFrame containing the data to be cleaned.

        Returns:
        pd.DataFrame: A DataFrame containing the cleaned data.
        """
        pass

class DropMissingValuesStrategy(DataCleaningStrategy):
    def clean(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Drop missing values from the DataFrame.

        Parameters:
        data (pd.DataFrame): A DataFrame containing the data to be cleaned.

        Returns:
        pd.DataFrame: A DataFrame containing the cleaned data.
        """
        return data.dropna()

class FillMissingValuesStrategy(DataCleaningStrategy):
    