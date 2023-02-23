import pandas as pd
from azureml.train.estimator import Estimator
from azureml.core.run import Run
from azureml.automl.core.shared import constants
from azureml.core import Workspace, Dataset
from azureml.data.datapath import DataPath


def get_result_df(remote_run):
    children = list(remote_run.get_children(recursive=True))
    summary_df = pd.DataFrame(
        index=["run_id", "run_algorithm", "primary_metric", "Score"]
    )
    goal_minimize = False
    for run in children:
        if (
            run.get_status().lower() == constants.RunState.COMPLETE_RUN
            and "run_algorithm" in run.properties
            and "score" in run.properties
        ):
            # We only count in the completed child runs.
            summary_df[run.id] = [
                run.id,
                run.properties["run_algorithm"],
                run.properties["primary_metric"],
                float(run.properties["score"]),
            ]
            if "goal" in run.properties:
                goal_minimize = run.properties["goal"].split("_")[-1] == "min"

    summary_df = summary_df.T.sort_values("Score", ascending=goal_minimize)
    summary_df = summary_df.set_index("run_algorithm")
    return summary_df


def run_inference(
    test_experiment,
    compute_target,
    script_folder,
    train_run,
    test_dataset,
    lookback_dataset,
    max_horizon,
    target_column_name,
    time_column_name,
    freq,
):
    model_base_name = "model.pkl"
    if "model_data_location" in train_run.properties:
        model_location = train_run.properties["model_data_location"]
        _, model_base_name = model_location.rsplit("/", 1)
    train_run.download_file(
        "outputs/{}".format(model_base_name), "inference/{}".format(model_base_name)
    )

    inference_env = train_run.get_environment()

    est = Estimator(
        source_directory=script_folder,
        entry_script="infer.py",
        script_params={
            "--max_horizon": max_horizon,
            "--target_column_name": target_column_name,
            "--time_column_name": time_column_name,
            "--frequency": freq,
            "--model_path": model_base_name,
        },
        inputs=[
            test_dataset.as_named_input("test_data"),
            lookback_dataset.as_named_input("lookback_data"),
        ],
        compute_target=compute_target,
        environment_definition=inference_env,
    )

    run = test_experiment.submit(
        est,
        tags={
            "training_run_id": train_run.id,
            "run_algorithm": train_run.properties["run_algorithm"],
            "valid_score": train_run.properties["score"],
            "primary_metric": train_run.properties["primary_metric"],
        },
    )

    run.log("run_algorithm", run.tags["run_algorithm"])
    return run


def get_step_args(
    partition_column_names,
    output_path,
    partitioned_dataset_name,
    input_data_path,
    datastore_name,
):
    """
    Returns the arguments for PythonScriptStep.

    :param partition_column_names:  The names of columns that were used for partitioning the data.
    :type partition_column_names: List[str]
    :param output_path: Output path for partitioned data.
    :type output_path: azureml.data.output_dataset_config.OutputFileDatasetConfig
    :param partitioned_dataset_name: Name that will be used for partitioned data.
    :type partitioned_dataset_name: str
    :param input_data_path: Relative path to the datastore where unpartitioned data is residing.
    :type input_data_path: str
    :param datastore_name: Name of the datastore where we have the data.
    :type datastore_name: str

    :return: A list of arguments.
    :rtype: List[str]
    """
    step_args = []
    step_args.append("--partition_column_names")
    step_args.extend(partition_column_names)
    step_args.append("--output_path")
    step_args.append(output_path)
    step_args.append("--partitioned_dataset_name")
    step_args.append(partitioned_dataset_name)
    step_args.append("--input_data_path")
    step_args.append(input_data_path)
    step_args.append("--datastore_name")
    step_args.append(datastore_name)
    return step_args


def get_prs_args(
    time_column_name,
    time_series_id_column_names,
    test_split=0.2,
    valid_split=0.2,
    first_task_creation_timeout=2400,
):
    """
    Returns the arguments for PRS.

    :param time_column_name: The name of the time column.
    :type time_column_name: str
    :param time_series_id_column_names: The names of columns used to uniquely indentify a timeseries.
    :type time_series_id_column_names: List[str]
    :param test_split: Ratio for test data.
    :type test_split: float
    :param valid_split: Ratio for validation data.
    :type valid_split: float
    :param first_task_creation_timeout: The timeout in second for monitoring the time between the
        job start to the run of first mini-batch.
    :type first_task_creation_timeout: int

    :return: A list of arguments.
    :rtype: List[Union[str, float]]
    """
    prs_args = []
    prs_args.append("--time_column_name")
    prs_args.append(time_column_name)
    prs_args.append("--time_series_id_column_names")
    prs_args.extend(time_series_id_column_names)
    prs_args.append("--test_split")
    prs_args.append(test_split)
    prs_args.append("--valid_split")
    prs_args.append(valid_split)
    prs_args.append("--first_task_creation_timeout")
    prs_args.append(first_task_creation_timeout)
    prs_args.append("--logging level")
    prs_args.append("WARNING")
    return prs_args


def register_dataset(
    workspace, src_dir, target, name, partition_column_names=None, overwrite=True
):
    """
    Register a dataset in the workspace.

    :param workspace: The workspace in which the Dataset will be registered.
    :type workspace: azureml.core.Workspace
    :param src_dir: The local directory to upload.
    :type src_dir: str
    :param target: Relative path to the datastore where files will be uploaded to.
    :type target: str
    :param partition_column_names: The names of columns that will be used for partitioning the data.
    :type partition_column_names: List[str]
    :param overwrite: Whether to overwrite the existing data.
    :type overwrite: bool

    :return: The registered dataset.
    :rtype: azureml.data.TabularDataset
    """
    datastore = workspace.get_default_datastore()

    Dataset.File.upload_directory(
        src_dir=src_dir,
        target=DataPath(datastore, target + "/raw"),
        overwrite=overwrite,
    )
    data = Dataset.Tabular.from_delimited_files(
        path=DataPath(datastore, target + "/raw")
    )
    if not partition_column_names:
        return data.register(workspace=workspace, name=name)
    else:
        return data.partition_by(
            partition_keys=partition_column_names,
            partition_as_file_dataset=False,
            target=(datastore, target + "/partitioned"),
            name=name,
        )


def get_partition_str(time_series_id_column_names):
    """
    Returns partition details in str format.

    :param time_series_id_column_names: The names of columns used to uniquely indentify a timeseries.
    :type time_series_id_column_names: List[str]

    :return: The registered dataset.
    :rtype: Tuple[str, str]
    """
    partition_format = ""
    partition_path = ""
    for key in time_series_id_column_names:
        partition_format = partition_format + "{" + key + "}/"
        partition_path += "*/"
    partition_format += "*.parquet"
    partition_path += "*.parquet"
    return partition_format, partition_path
