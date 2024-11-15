from abc import ABC, abstractmethod

class BaseProcessor(ABC):
    """
    An abstract base class for processing data. This class cannot be instantiated directly.
    Subclasses must implement the `process` and `save_to_django` methods.
    """

    def __init__(
        self,
        file_path: str = "path/to/file",  # Default file path for data
        batch_size: int = 1000,  # Default batch size for processing
    ) -> None:
        """
        Initializes the base processor with the provided file path and batch size.
        :param file_path: The path to the data file to be processed (default: "path/to/file").
        :param batch_size: The number of records to process in each batch (default: 1000).
        """
        self.file_path = file_path
        self.batch_size = batch_size

    @abstractmethod
    def process(self):
        """
        Subclasses must implement this method to process the data.
        This method should read from the data source, process the data, and possibly return the processed data (e.g., as a dataframe).
        """
        pass

    @abstractmethod
    def save_to_django(self, df):
        """
        Subclasses must implement this method to save the processed data to Django models.
        :param df: The processed data, typically in the form of a spark DataFrame, to be saved to Django models.
        """
        pass
