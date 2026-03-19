from prefect import flow
from bronze_ingestion_flow import bronze_ingestion_flow
from silver_transformation_flow import silver_transformation_flow


@flow(name="eversys-bronze-silver-pipeline")
def eversys_pipeline():
    bronze_ingestion_flow()
    silver_transformation_flow()


if __name__ == "__main__":
    eversys_pipeline()