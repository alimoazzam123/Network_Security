import json
import sys
import os
import pandas as pd
import numpy as np
from scipy.stats import ks_2samp
from pandas import DataFrame
from src.utils.main_utils import read_yaml_file,write_yaml_file
from src.exception import MyException
from src.logger import logging
from src.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from src.entity.config_entity import DataValidationConfig
from src.constants import SCHEMA_FILE_PATH 

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config=read_yaml_file(file_path=SCHEMA_FILE_PATH)
        except Exception as e:
            raise MyException(e,sys)
    
    def validate_number_of_columns(self,dataframe:DataFrame)->bool:
        try:
            number_of_columns=len(self._schema_config)
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            return False
        except Exception as e:
            raise MyException(e,sys)
    
    def is_column_exist(self, df: DataFrame) -> bool:
        try:
            dataframe_columns = df.columns
            missing_numerical_columns = []

            # Check if all required numerical columns are present in the dataframe
            for column in self._schema_config["numerical_columns"]:
                if column not in dataframe_columns:
                    missing_numerical_columns.append(column)

            # Assume that all columns are present initially
            status = True

            # If any columns are missing, update status to False
            if len(missing_numerical_columns) > 0:
                status = False
                logging.info(f"Missing numerical column(s): {missing_numerical_columns}")

            return status
        except Exception as e:
            raise MyException(e, sys)

    def detect_dataset_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}
            for column in base_df.columns:
                d1=base_df[column]
                d2=current_df[column]
                is_sample_dist=ks_2samp(d1,d2)
                if threshold<=is_sample_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({
                    column:{
                        "p_value":float(is_sample_dist.pvalue),
                        "drift_status":is_found
                    } 
                })
            drift_report_file_path=self.data_validation_config.drift_report_file_path
            dir_path=os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path,content=report)
            return status
        except Exception as e:
            raise MyException(e,sys)

    @staticmethod
    def read_data(file_path)->DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise MyException(e,sys)
        
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            validation_error_msg=""
            logging.info("Starting data validation")
            train_df, test_df = (DataValidation.read_data(file_path=self.data_ingestion_artifact.trained_file_path),
                                 DataValidation.read_data(file_path=self.data_ingestion_artifact.test_file_path))
            
            status=self.validate_number_of_columns(dataframe=train_df)
            if not status:
                validation_error_msg += f"Columns are missing in training dataframe."
            else:
                logging.info(f"All required columns present in training dataframe:{status}")
            
            status=self.validate_number_of_columns(dataframe=test_df)
            if not status:
                validation_error_msg += f"Columns are missing in testing dataframe."
            else:
                logging.info(f"All required columns present in testing dataframe:{status}")
            
            status=self.is_column_exist(df=train_df)
            if not status:
                validation_error_msg += f"Columns are missing in training dataframe."
            else:
                logging.info(f"All required columns are present in training dataframe:{status}")
            
            status=self.is_column_exist(df=test_df)
            if not status:
                validation_error_msg +=f"Columns are missing in testing dataframe."
            else:
                logging.info(f"All columns are pr esent in testing dataframe:{status}")

            status=self.detect_dataset_drift(base_df=train_df,current_df=test_df)
            dir_path=os.path. dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)
            train_df.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
            test_df.to_csv(self.data_validation_config.valid_test_file_path,index=False,header=True)

            data_validation_artifact=DataValidationArtifact(
                validation_status=status,
                validation_error_msg=validation_error_msg,
                valid_train_file_path=self.data_validation_config.valid_train_file_path,
                valid_test_file_path=self.data_validation_config.valid_test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )
            return data_validation_artifact
        except Exception as e:
            raise MyException(e,sys)