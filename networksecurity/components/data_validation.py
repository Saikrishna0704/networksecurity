from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

from networksecurity.entity.artifact_entity import DataIngestionArtifact , DataValidationArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.constants.training_pipeline import SCHEMA_FILE_PATH

from scipy.stats import ks_2samp
import pandas as pd
import os, sys

from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file

#Creating a Data Validation class.
#Takes data ingestion artifact and data avlidation config as arguments
class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config:DataValidationConfig):
        
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH) #Reading schema info from the yaml file 
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        


    @staticmethod
    def read_data(file_path) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    #function to check if the number of columns are same. Using columns in Schema.yaml file and the current dataframe.         
    def validate_number_of_columns(self, dataframe:pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self._schema_config)
            logging.info(f"required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.columns)}")
            if len(dataframe.columns) == number_of_columns:
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e,sys)    
         
    #function to check the distribution of the columns. 
    def detect_data_drift(self, base_df, current_df, thereshold = 0.05) -> bool:
        try:
            status = True
            report = {}
            for column in base_df.columns:
                d1 =  base_df[column]
                d2 = current_df[column]
                is_same_dist = ks_2samp(d1,d2)
                if thereshold < is_same_dist.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False 
                #Making note of the p value and the corresponding status. 
                report.update({column:{
                    "p_value": float(is_same_dist.pvalue),
                    "drift_status": is_found
                    }                                   
                             })    
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
        drift_report_file_path = self.data_validation_config.drift_report_file_path

        #Creating Directory

        dir_path = os.path.dirname(drift_report_file_path)
        os.makedirs(dir_path, exist_ok = True)
        write_yaml_file(file_path=drift_report_file_path,content= report)

    #Main function that drived the data validation. 
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path

            train_dataframe =  DataValidation.read_data(train_file_path)
            test_dataframe =  DataValidation.read_data(test_file_path)

            #validating number of columns
            status = self.validate_number_of_columns(dataframe=train_dataframe)
            if not status:
                error_message = f"Train Dataframe does not contain all columns. \n"
            status = self.validate_number_of_columns(dataframe=train_dataframe)    
            if not status:
                error_message = f"Test Dataframe does not contain all columns. \n"
                
            #Checking data drift
            status = self.detect_data_drift(base_df= train_dataframe, current_df=test_dataframe)
            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok = True)

            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path, index=False, header=True
            )

            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path, index=False, header=True
            )
            
            # Validation classs initialization
            data_validation_artifact = DataValidationArtifact(
                validation_status=status, #Drift status is actually used here to check
                valid_train_file_path=self.data_ingestion_artifact.train_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,
            )
            return data_validation_artifact


        except Exception as e:
            raise NetworkSecurityException(e, sys)
        