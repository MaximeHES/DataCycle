# pipeline_flow.py
from prefect import flow
from bronze_ingestion_flow import bronze_ingestion_flow
from silver_transformation_flow import silver_transformation_flow


@flow(name="eversys-bronze-silver-pipeline", log_prints=True)
def eversys_pipeline():
    print("Starting bronze ingestion...")
    bronze_ingestion_flow()

    print("Bronze done. Starting silver transformation...")
    silver_transformation_flow()

    print("Pipeline complete.")


if __name__ == "__main__":
    eversys_pipeline.serve(
        name="eversys-pipeline-scheduled",
        cron="*/30 * * * *",  # every 30 minutes
        pause_on_shutdown=False,
    )