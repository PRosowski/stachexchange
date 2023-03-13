#import libraries
from io import StringIO
import pandas as pd
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#You can inherit from other operator of course
class XMLToParquet(BaseOperator):
    def __init__(
        self,
        *,
        bucket: str,
        prefix: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        encode = "utf-8",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.gcp_conn_id = gcp_conn_id
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list:

        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info(
            "Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s",
            self.bucket,
            self.delimiter,
            self.prefix,
        )
        list_of_files = hook.list(bucket_name=self.bucket, prefix=self.prefix, delimiter=self.delimiter)
        for input_file in list_of_files:
            self.save_to_parquet(hook,self.parse_file(hook, input_file), f"{input_file}_output.parquet")

    def parse_file(self, hook, input_file_name):
        file_byte_array = hook.download_as_byte_array(self.bucket, input_file_name)
        s=str(file_byte_array,encoding=self.encode)
        data = StringIO(s)
        data_df = pd.read_xml(data)
        return data_df
    
    def save_to_parquet(self, hook:GCSHook, input_df: pd.DataFrame, output_file_name:str) -> None:
        input_df.to_parquet(output_file_name)
        
    
