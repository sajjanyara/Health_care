from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as f
from datetime import timedelta
from airflow.models import Variable


def extract_data():
    # Initialize Spark session
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

    # Read data
    conditions_df = pd.read_excel("conditions.xlsx", sheet_name="Sheet1")
    conditions_df = spark.createDataFrame(conditions_df)

    patients_df = spark.read.format("csv").option("header", True).load("patients.csv")
    patients_gender_df = spark.read.format("csv").option("header", True).load("patient_gender.csv")
    symptoms_df = spark.read.format("csv").option("header", True).load("symptoms.csv")
    medications_df = spark.read.format("csv").option("header", True).load("medications.csv")
    encounters_df = spark.read.load("encounters.parquet")

    return {
        'conditions_df': conditions_df,
        'patients_df': patients_df,
        'patients_gender_df': patients_gender_df,
        'symptoms_df': symptoms_df,
        'medications_df': medications_df,
        'encounters_df': encounters_df
    }


def transform_data(**kwargs):
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    ti = kwargs['ti']

    # Pull data from XCom
    data = ti.xcom_pull(task_ids='extract_data')

    conditions_df = data['conditions_df']
    patients_df = data['patients_df']
    patients_gender_df = data['patients_gender_df']
    symptoms_df = data['symptoms_df']
    medications_df = data['medications_df']
    encounters_df = data['encounters_df']

    # Data Processing
    patients_df = patients_df.drop('GENDER')
    patients_df_with_gender = patients_df.join(
        patients_gender_df,
        patients_df["PATIENT_ID"] == patients_gender_df["Id"],
        "left"
    ).select(
        patients_df["*"],
        patients_gender_df["GENDER"]
    )

    columns = patients_df_with_gender.columns
    columns.remove("GENDER")
    new_columns = columns[:14] + ["GENDER"] + columns[14:]
    patients_df_with_gender = patients_df_with_gender[new_columns]

    # Cleansing and Renaming Columns
    def rename_columns(df, rename_dict):
        for old_col, new_col in rename_dict.items():
            df = df.withColumnRenamed(old_col, new_col)
        return df

    conditions_rename_dict = {
        "START": "onset_date",
        "STOP": "resolved_date",
        "PATIENT": "patient_id",
        "ENCOUNTER": "encounter_id",
        "CODE": "conditions_source_code",
        "DESCRIPTION": "conditions_normalized_description"
    }

    medications_rename_dict = {
        "START": "dispensing_date",
        "STOP": "medication_stop_date",
        "PATIENT": "patient_id",
        "PAYER": "provider_id",
        "ENCOUNTER": "encounter_id",
        "CODE": "medication_source_code",
        "DESCRIPTION": "medication_source_description",
        "BASE_COST": "base_cost",
        "PAYER_COVERAGE": "payer_coverage",
        "DISPENSES": "quantity",
        "TOTALCOST": "total_cost",
        "REASONCODE": "medication_reason_code",
        "REASONDESCRIPTION": "medication_reason_description"
    }

    symptoms_rename_dict = {
        "PATIENT": "patient_id",
        "GENDER": "gender",
        "RACE": "race",
        "ETHNICITY": "ethnicity",
        "AGE_BEGIN": "observations_age_start",
        "AGE_END": "observations_age_end",
        "PATHOLOGY": "result",
        "NUM_SYMPTOMS": "no_of_symptoms",
        "SYMPTOMS": "symptoms"
    }

    patients_rename_dict = {
        "PATIENT_ID": "patient_id",
        "BIRTHDATE": "birth_date",
        "DEATHDATE": "death_date",
        "SSN": "SOCIAL_SECURITY_NUMBER",
        "DRIVERS": "drivers_name_plate",
        "PASSPORT": "passport_number",
        "PREFIX": "prefix",
        "FIRST": "first_name",
        "LAST": "last_name",
        "SUFFIX": "suffix",
        "MAIDEN": "maiden_name",
        "MARITAL": "marital_status",
        "RACE": "race",
        "ETHNICITY": "ethnicity",
        "GENDER": "sex",
        "BIRTHPLACE": "birthplace",
        "ADDRESS": "address",
        "CITY": "city",
        "STATE": "state",
        "COUNTY": "county",
        "FIPS": "fips",
        "ZIP": "zip_code",
        "LAT": "latitude",
        "LON": "longitude",
        "HEALTHCARE_EXPENSES": "healthcare_expenses",
        "HEALTHCARE_COVERAGE": "healthcare_coverage",
        "INCOME": "income"
    }

    encounters_rename_dict = {
        "Id": "encounter_id",
        "START": "encounter_start_date",
        "STOP": "encounter_end_date",
        "PATIENT": "patient_id",
        "ORGANIZATION": "facility_id",
        "PROVIDER": "attending_provider_id",
        "PAYER": "attending_provider_name",
        "ENCOUNTERCLASS": "encounter_type",
        "CODE": "admit_source_code",
        "DESCRIPTION": "admit_source_description",
        "BASE_ENCOUNTER_COST": "charge_amount",
        "TOTAL_CLAIM_COST": "allowed_amount",
        "PAYER_COVERAGE": "paid_amount",
        "REASONCODE": "primary_diagnosis_code",
        "REASONDESCRIPTION": "primary_diagnosis_description"
    }

    conditions_df = rename_columns(conditions_df, conditions_rename_dict)
    medications_df = rename_columns(medications_df, medications_rename_dict)
    symptoms_df = rename_columns(symptoms_df, symptoms_rename_dict)
    patients_df = rename_columns(patients_df_with_gender, patients_rename_dict)
    encounters_df = rename_columns(encounters_df, encounters_rename_dict)

    # Additional transformations
    patients_df = patients_df.withColumn("first_name", f.regexp_replace("first_name", "\\d+", ""))
    patients_df = patients_df.withColumn("last_name", f.regexp_replace("last_name", "\\d+", ""))
    patients_df = patients_df.withColumn("maiden_name", f.regexp_replace("maiden_name", "\\d+", ""))
    patients_df = patients_df.withColumn("patient_id", f.lower(f.col("patient_id")))
    patients_df = patients_df.withColumn("fips", f.col("fips").cast("int"))

    conditions_df = conditions_df.withColumn("patient_id", f.lower(f.col("patient_id")))
    conditions_df = conditions_df.withColumn("encounter_id", f.lower(f.col("encounter_id")))

    medications_df = medications_df.withColumn("patient_id", f.lower(f.col("patient_id")))
    medications_df = medications_df.withColumn("encounter_id", f.lower(f.col("encounter_id")))
    medications_df = medications_df.withColumn("medication_reason_code", f.col("medication_reason_code").cast("int"))

    symptoms_df = symptoms_df.withColumn("patient_id", f.lower(f.col("patient_id")))

    symptom_pattern = r'([^:]+):(\d+)'
    symptom_columns = ["Rash", "Joint Pain", "Fatigue", "Fever"]
    symptoms_df = symptoms_df.withColumn("symptom_explode", f.explode(f.split(f.col("symptoms"), ";")))
    symptoms_df = symptoms_df.withColumn("symptom_name", f.regexp_extract(f.col("symptom_explode"), symptom_pattern, 1)) \
        .withColumn("symptom_value", f.regexp_extract(f.col("symptom_explode"), symptom_pattern, 2))
    symptoms_df = symptoms_df.groupBy("patient_id", "gender", "race", "ethnicity", "observations_age_start",
                                      "observations_age_end", "result") \
        .pivot("symptom_name") \
        .agg(f.max("symptom_value").alias("value"))
    symptoms_df = symptoms_df.groupBy("patient_id", "gender", "race", "ethnicity", "observations_age_start",
                                      "observations_age_end") \
        .pivot("result") \
        .agg(*[f.max(col).alias(col) for col in symptom_columns])

    for col_name in symptoms_df.columns:
        new_col_name = col_name.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")
        symptoms_df = symptoms_df.withColumnRenamed(col_name, new_col_name)

    encounters_df = encounters_df.withColumn("patient_id", f.lower(f.col("patient_id")))
    encounters_df = encounters_df.withColumn("encounter_id", f.lower(f.col("encounter_id")))
    encounters_df = encounters_df.withColumn("primary_diagnosis_code", f.col("primary_diagnosis_code").cast("int"))

    # Return transformed data for further processing
    return {
        'patients_df': patients_df,
        'conditions_df': conditions_df,
        'medications_df': medications_df,
        'symptoms_df': symptoms_df,
        'encounters_df': encounters_df
    }


def load_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='transform_data')

    patients_df = data['patients_df']
    conditions_df = data['conditions_df']
    medications_df = data['medications_df']
    symptoms_df = data['symptoms_df']
    encounters_df = data['encounters_df']

    # ======== Data quality checks with great expectations before loading data to Database ========
    import great_expectations as gx
    from great_expectations import data_context
    context = gx.get_context(context_root_dir="/home/great_expectations/")
    suite = context.create_expectation_suite(expectation_suite_name="patients_suite", overwrite_existing=True)
    from great_expectations.core.expectation_configuration import ExpectationConfiguration
    expectation_configuration_1 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "PATIENT_ID",
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration_1)
    expectation_configuration_2 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "SSN",
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration_2)
    expectation_configuration_3 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={
            "column": "PATIENT_ID",
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration_3)

    expectation_configuration_4 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={
            "column": "SSN",
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration_4)
    expectation_configuration_5 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={
            "column": "PASSPORT",
        },
    )
    suite.add_expectation(expectation_configuration=expectation_configuration_5)
    context.save_expectation_suite(expectation_suite=suite)
    from great_expectations.checkpoint import SimpleCheckpoint
    context = data_context.DataContext('/Users/sajjannarsinghani/Documents/gx_tutorials/great_expectations')
    checkpoint_name = "my_checkpoint"
    checkpoint_config = context.add_checkpoint(
        name=checkpoint_name,
        config_version="1",
        class_name="SimpleCheckpoint",
        validations=[
            {
                "expectation_suite_name": "patients_suite",
                "action_list": [
                    {
                        "name": "StoreValidationResultAction",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "StoreEvaluationParametersAction",
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                    },
                    {
                        "name": "UpdateDataDocsAction",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    },
                ],
            }
        ],
    )
    context.add_or_update_checkpoint(**checkpoint_config)
    datasource_name = "my_pandas_datasource"
    asset_name = "patients_profile_data"
    context.delete_datasource(datasource_name=datasource_name)
    datasource = context.sources.add_pandas(name=datasource_name)
    data_asset = datasource.add_dataframe_asset(name=asset_name, dataframe=patients_df.toPandas())
    checkpoint = context.get_checkpoint(name=checkpoint_name)
    result_format = {"result_format": "COMPLETE"}
    results = context.run_checkpoint(checkpoint_name=checkpoint_name, result_format=result_format)
    # ======== *** ========

    # Database connection
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    username = Variable.get('username')
    password = Variable.get('password')
    host = Variable.get('host')

    dataset_list = ["patients", "conditions", "medications", "encounters", "symptoms"]
    for table_name in dataset_list:
        df = data[f'{table_name}_df']
        df.write.format("jdbc"). \
            option('url', host). \
            option('driver', 'org.postgresql.Driver'). \
            option('dbtable', f'presentation_layer.{table_name}'). \
            option('user', username). \
            option('password', password). \
            mode('append').save()


# Wide Table Processing
def process_wide_table(**kwargs):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as f

    spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
    ti = kwargs['ti']

    # Pull data from XCom
    data = ti.xcom_pull(task_ids='transform_data')

    patients_df = data['patients_df']
    conditions_df = data['conditions_df']
    medications_df = data['medications_df']
    symptoms_df = data['symptoms_df']

    def merge_datasets(df1, df2, key):
        return df1.join(df2, df1[key] == df2[key], "left").drop(df2[key])

    patients_selected = patients_df.select(
        "patient_id",
        "birth_date",
        "death_date",
        "social_security_number",
        "drivers_name_plate",
        "passport_number",
        "prefix",
        "first_name",
        "last_name",
        "suffix",
        "maiden_name",
        "marital_status",
        "race",
        "ethnicity",
        "sex",
        "birthplace",
        "address",
        "city",
        "state",
        "county",
        "fips",
        "zip_code",
        "latitude",
        "longitude",
        "healthcare_expenses",
        "healthcare_coverage",
        "income"
    )
    conditions_selected = conditions_df.select(
        "patient_id",
        "onset_date",
        "resolved_date",
        "encounter_id",
        "conditions_source_code",
        "conditions_normalized_description"
    )
    medications_selected = medications_df.select(
        "patient_id",
        "dispensing_date",
        "medication_stop_date",
        "provider_id",
        "encounter_id",
        "medication_source_code",
        "medication_source_description",
        "base_cost",
        "payer_coverage",
        "quantity",
        "total_cost",
        "medication_reason_code",
        "medication_reason_description"
    )
    symptoms_selected = symptoms_df.select(
        "patient_id",
        "gender",
        "race",
        "ethnicity",
        "observations_age_start",
        "observations_age_end",
        "rash",
        "joint_pain",
        "fatigue",
        "fever"
    )

    wide_table = merge_datasets(patients_selected, conditions_selected, "patient_id")
    wide_table = merge_datasets(wide_table, medications_selected, "patient_id")
    wide_table = merge_datasets(wide_table, symptoms_selected, "patient_id")

    # Write to database
    wide_table.write.format("jdbc"). \
        option('url', 'jdbc:postgresql://your-dwh-host/your-dwh-db'). \
        option('driver', 'org.postgresql.Driver'). \
        option('dbtable', 'presentation_layer.wide_table'). \
        option('user', 'your_username'). \
        option('password', 'your_password'). \
        mode('append').save()

    print("Wide table written to DWH.")


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='An ETL pipeline DAG',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

process_wide_table = PythonOperator(
    task_id='wide_table',
    python_callable=process_wide_table,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> extract >> transform >> load >> process_wide_table >> end
