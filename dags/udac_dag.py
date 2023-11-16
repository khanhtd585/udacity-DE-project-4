from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from operators import (LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CreateTableOperator, StageToRedshiftOperator)
from helpers import SqlQueries
from airflow import DAG


redshift_conn_id = "redshift"
aws_credentials_id = "aws_credentials"
s3_bucket = Variable.get("s3_bucket")
vande = ["time","artists", "songs", "users"]
default_args = {
    'owner': 'Khanh585',
}
with DAG(
        dag_id='a_final-dag-v1.8',
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        start_date=datetime.now(),
        catchup=False
) as dag:
    start_operator = EmptyOperator(task_id='Begin_execution',  dag=dag)

    # create_table = CreateTableOperator(
    #     task_id='Create_table',
    #     dag=dag,
    #     redshift_conn_id=redshift_conn_id,
    #     aws_credentials=aws_credentials_id,
    #     sql=SqlQueries.create_table
    # )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        aws_credentials=aws_credentials_id,
        table="staging_events",
        s3_bucket=s3_bucket,
        s3_key="log_data",
        format_json="s3://udacity-dend/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        aws_credentials=aws_credentials_id,
        table="staging_songs",
        s3_bucket=s3_bucket,
        s3_key="song_data",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="songplays",
        sql_load=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="users",
        sql_load=SqlQueries.user_table_insert,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="songs",
        sql_load=SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="artists",
        sql_load=SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="time",
        sql_load=SqlQueries.time_table_insert,
    )

    run_quality_checks = DataQualityOperator(
        redshift_conn_id=redshift_conn_id,
        task_id='Check_quality',
        dag=dag,
        tables=["time", "artists", "songs","users", "songplays", "staging_songs", "staging_events"]
    )

    end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,  
                            load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator
