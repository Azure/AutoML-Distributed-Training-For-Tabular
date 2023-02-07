import argparse

from azureml.core import Run, Datastore, Dataset
from azureml.data import TabularDataset, FileDataset


def my_parse_args():
    parser = argparse.ArgumentParser("Arg Parser")

    parser.add_argument("--partition_column_names", nargs="*", type=str, required=True)
    parser.add_argument("--output_path", type=str, required=True)
    parser.add_argument("--partitioned_dataset_name", type=str, required=True)
    parser.add_argument("--input_data_path", type=str, required=True)
    parser.add_argument("--datastore_name", type=str, required=True)

    args, _ = parser.parse_known_args()
    return args


def partition_required(dataset, partition_column_names):
    if isinstance(dataset, FileDataset):
        return False
    elif isinstance(dataset, TabularDataset):
        if set(partition_column_names).issubset(set(dataset.partition_keys)):
            return False
        return True
    return True


def partition_dataset(
    input_dataset, partition_column_names, partitioned_dataset_name, datastore
):
    print(
        f"Performing dataset partition with partition columns: {partition_column_names}"
    )
    input_dataset.partition_by(
        partition_keys=partition_column_names,
        partition_as_file_dataset=False,
        target=(datastore, partitioned_dataset_name),
        name=partitioned_dataset_name,
    )


def main():
    args = my_parse_args()
    partition_column_names = args.partition_column_names
    partitioned_dataset_name = args.partitioned_dataset_name
    input_data_path = args.input_data_path
    datastore_name = args.datastore_name

    run_context = Run.get_context()
    ws = run_context.experiment.workspace

    datastore = Datastore.get(ws, datastore_name)
    input_dataset = Dataset.Tabular.from_delimited_files(
        path=(datastore, input_data_path)
    )

    if partition_required(input_dataset, partition_column_names):
        partition_dataset(
            input_dataset, partition_column_names, partitioned_dataset_name, datastore
        )
    else:
        raise Exception("Dataset partitioning is not required.")


if __name__ == "__main__":
    main()
