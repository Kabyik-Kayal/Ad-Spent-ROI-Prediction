import pandas as pd
from pymongo import MongoClient
from abc import ABC, abstractmethod
from logger import logger


# Abstract Base Class for Data Ingestion Strategy
# ------------------------------------------------
# This class defines a common interface for different data ingestion strategies.
# Subclasses must implement the `ingest` method.
class DataIngestionStrategy(ABC):
    @abstractmethod
    def ingest(self, connection_details: dict) -> pd.DataFrame:
        """
        Abstract method to ingest data from a source into a DataFrame.

        Parameters:
        connection_details (dict): A dictionary containing connection details for the data source.

        Returns:
        pd.DataFrame: A dataframe containing the ingested data.
        """
        pass


# Concrete Strategy to ingest data from a MongoDB server
# --------------------------------------------------------
class MongoDBIngestionStrategy(DataIngestionStrategy):
    def ingest(self, connection_details: dict) -> pd.DataFrame:
        """
        Ingest data from a MongoDB server.

        Parameters:
        connection_details (dict): A dictionary containing connection details for MongoDB.
            Required keys: 'uri', 'database', 'collection'

        Returns:
        pd.DataFrame: A dataframe containing the ingested data.
        """
        logger.info("Connecting to MongoDB...")
        try:
            # Extract connection details
            uri = connection_details.get('uri')
            database_name = connection_details.get('database')
            collection_name = connection_details.get('collection')

            # Validate the details
            if not all([uri, database_name, collection_name]):
                raise ValueError("Missing connection details. Ensure 'uri', 'database', and 'collection' are provided.")

            # Connect to MongoDB
            client = MongoClient(uri)
            database = client[database_name]
            collection = database[collection_name]

            # Fetch data from the collection
            data = list(collection.find())

            # Convert data to DataFrame
            df = pd.DataFrame(data)

            # Drop the MongoDB '_id' field if present
            if '_id' in df.columns:
                df.drop(columns=['_id'], inplace=True)

            logger.info("Data ingestion from MongoDB completed successfully.")
            return df
        except Exception as e:
            logger.error(f"Error during MongoDB ingestion: {e}")
            raise


# Context Class for Data Ingestion
# ---------------------------------
class DataIngestor:
    def __init__(self, strategy: DataIngestionStrategy):
        """
        Initializes the DataIngestor with a specific data ingestion strategy.

        Parameters:
        strategy (DataIngestionStrategy): The strategy to be used for data ingestion.
        """
        self._strategy = strategy

    def set_strategy(self, strategy: DataIngestionStrategy):
        """
        Sets a new strategy for the DataIngestor.

        Parameters:
        strategy (DataIngestionStrategy): The new strategy to be used for data ingestion.
        """
        logger.info("Switching data ingestion strategy.")
        self._strategy = strategy

    def ingest_data(self, connection_details: dict) -> pd.DataFrame:
        """
        Executes the data ingestion using the current strategy.

        Parameters:
        connection_details (dict): A dictionary containing connection details for the data source.

        Returns:
        pd.DataFrame: A dataframe containing the ingested data.
        """
        logger.info("Ingesting data using the current strategy.")
        return self._strategy.ingest(connection_details)


# Example Usage
# -------------
if __name__ == "__main__":
    # # Define MongoDB connection details
    # mongo_details = {
    #     'uri': 'your_mongodb_connection_string',  # Replace with your MongoDB connection string
    #     'database': 'your_database_name',        # Replace with your database name
    #     'collection': 'your_collection_name'     # Replace with your collection name
    # }

    # # Create a MongoDB ingestion strategy
    # mongo_strategy = MongoDBIngestionStrategy()

    # # Initialize the DataIngestor with the MongoDB strategy
    # ingestor = DataIngestor(strategy=mongo_strategy)

    # # Ingest data from MongoDB
    # try:
    #     data_frame = ingestor.ingest_data(mongo_details)
    #     print("Data fetched successfully!")
    #     print(data_frame.head())
    # except Exception as e:
    #     logger.error(f"Failed to fetch data: {e}")
    pass