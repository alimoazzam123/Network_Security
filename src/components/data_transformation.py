import os
import sys
import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer
from sklearn.pipeline import Pipeline
from src.constants import TARGET_COLUMN
from src.constants import DATA_TRANSFORMATION_IMPUTER_PARAMS
from src.entity.config_entity import DataTransformationConfig
from src.entity.artifact_entity import DataValidationArtifact, DataTransformationArtifact, DataIngestionArtifact
from src.exception import MyException
from src.logger import logging
from src.utils.main_utils import save_object, save_numpy_array_data


class DataTransformation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact,
                 data_transformation_config: DataTransformationConfig,
                 data_validation_artifact: DataValidationArtifact):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_transformation_config = data_transformation_config
            self.data_validation_artifact = data_validation_artifact
        except Exception as e:
            raise MyException(e, sys)

    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise MyException(e, sys)

    def get_data_transformer_object(self) -> Pipeline:
        logging.info("Entered get_data_transformer_object method of DataTransformation class")
        try:
            imputer: KNNImputer = KNNImputer(**DATA_TRANSFORMATION_IMPUTER_PARAMS)
            logging.info(f"Initialized KNN imputer with params: {DATA_TRANSFORMATION_IMPUTER_PARAMS}")
            processor: Pipeline = Pipeline([("imputer", imputer)])
            logging.info("Final Pipeline ready")
            logging.info("Exited get_data_transformer_object method of DataTransformation class")
            return processor
        except Exception as e:
            raise MyException(e, sys)

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info("Data transformation started")

            if not self.data_validation_artifact.validation_status:
                raise Exception(self.data_validation_artifact.validation_error_msg)

            train_df = self.read_data(self.data_validation_artifact.valid_train_file_path)
            test_df = self.read_data(self.data_validation_artifact.valid_test_file_path)
            logging.info("Train-Test files loaded")

            # Prepare training dataframe
            input_feature_train_df = train_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_train_df = train_df[TARGET_COLUMN].replace(-1, 0)

            # Prepare testing dataframe
            input_feature_test_df = test_df.drop(columns=[TARGET_COLUMN], axis=1)
            target_feature_test_df = test_df[TARGET_COLUMN].replace(-1, 0)

            logging.info(f"Train Data Types: \n{input_feature_train_df.dtypes}")
            logging.info(f"Test Data Types: \n{input_feature_test_df.dtypes}")
            logging.info("Input and Target columns defined for both train and test datasets")

            # Data Transformation
            preprocessor = self.get_data_transformer_object()
            logging.info("Got the preprocessor object")

            logging.info("Applying transformation to training data")
            transformed_input_feature_train_arr = preprocessor.fit_transform(input_feature_train_df)

            logging.info("Applying transformation to testing data")
            transformed_input_feature_test_arr = preprocessor.transform(input_feature_test_df)

            logging.info("Transformation completed for train and test datasets")

            # Concatenate input and target arrays
            train_arr = np.c_[transformed_input_feature_train_arr, np.array(target_feature_train_df)]
            test_arr = np.c_[transformed_input_feature_test_arr, np.array(target_feature_test_df)]
            logging.info("Feature-target concatenation done for train and test arrays")

            # Save transformed arrays and preprocessor object
            save_numpy_array_data(self.data_transformation_config.transformed_train_file_path, array=train_arr)
            save_numpy_array_data(self.data_transformation_config.transformed_test_file_path, array=test_arr)
            save_object(self.data_transformation_config.transformed_object_file_path, preprocessor)

            # Create artifact
            data_transformation_artifact = DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config.transformed_object_file_path,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
            )

            logging.info("Data Transformation Artifact created successfully")
            return data_transformation_artifact

        except Exception as e:
            raise MyException(e, sys)
