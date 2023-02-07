from pathlib import Path
import argparse
import pandas as pd

from azureml_user.parallel_run import EntryScript


def my_parse_args():
    parser = argparse.ArgumentParser("Arg Parser")

    parser.add_argument("--time_column_name", type=str, required=True)
    parser.add_argument(
        "--time_series_id_column_names", nargs="*", type=str, required=True
    )
    parser.add_argument("--test_split", type=float, default=0.2)
    parser.add_argument("--valid_split", type=float, default=0.2)

    args, _ = parser.parse_known_args()
    return args


def split_fraction_by_grain(df, fraction, time_column_name, grain_column_names=None):
    if not grain_column_names:
        df["tmp_grain_column"] = "grain"
        grain_column_names = ["tmp_grain_column"]

    """Group df by grain and split on last n rows for each group."""
    df_grouped = df.sort_values(time_column_name).groupby(
        grain_column_names, group_keys=False
    )

    df_head = df_grouped.apply(
        lambda dfg: dfg.iloc[: -int(len(dfg) * fraction)] if fraction > 0 else dfg
    )

    df_tail = df_grouped.apply(
        lambda dfg: dfg.iloc[-int(len(dfg) * fraction) :] if fraction > 0 else dfg[:0]
    )

    if "tmp_grain_column" in grain_column_names:
        for df2 in (df, df_head, df_tail):
            df2.drop("tmp_grain_column", axis=1, inplace=True)

        grain_column_names.remove("tmp_grain_column")

    return df_head, df_tail


def split_full_for_forecasting(df, time_column_name, split, grain_column_names=None):
    index_name = df.index.name

    # Assumes that there isn't already a column called tmpindex
    df["tmpindex"] = df.index

    train_df, test_df = split_fraction_by_grain(
        df, split, time_column_name, grain_column_names
    )

    train_df = train_df.set_index("tmpindex")
    train_df.index.name = index_name

    test_df = test_df.set_index("tmpindex")
    test_df.index.name = index_name

    df.drop("tmpindex", axis=1, inplace=True)

    return train_df, test_df


def init():
    global test_split, valid_split
    global time_column_name, time_series_id_column_names
    global logger, output_dir, no_of_grain_columns

    args = my_parse_args()
    time_column_name = args.time_column_name
    time_series_id_column_names = args.time_series_id_column_names
    test_split = args.test_split
    valid_split = args.valid_split

    no_of_grain_columns = len(time_series_id_column_names)

    entry_script = EntryScript()
    logger = entry_script.logger
    output_dir = Path(entry_script.output_dir)


def run(mini_batch):
    result_list = []

    for file in mini_batch:
        file_ext = file.split(".")[-1]
        grain_array = file.split(f".{file_ext}")[0].split("/")[
            -no_of_grain_columns - 1 : -1
        ]
        grain_dir = "/".join(grain_array)
        if file_ext == "parquet":
            df = pd.read_parquet(file)
        elif file_ext == "csv":
            df = pd.read_csv(file, parse_dates=[time_column_name])
        else:
            print(
                f"Unrecognized file format. Expected either 'parquet' or 'csv', received '{file_ext}'"
            )
            continue
        if test_split > 0:
            train_df, test_df = split_full_for_forecasting(
                df, time_column_name, test_split
            )
            test_path = output_dir / Path("test") / Path(grain_dir)
            test_path.mkdir(parents=True, exist_ok=True)
            test_df.to_parquet(
                test_path / "part-0.parquet", compression=None, index=False
            )
        else:
            train_df = df
        if valid_split > 0:
            train_df, valid_df = split_full_for_forecasting(
                train_df, time_column_name, valid_split
            )
            valid_path = output_dir / Path("valid") / Path(grain_dir)
            valid_path.mkdir(parents=True, exist_ok=True)
            valid_df.to_parquet(
                valid_path / "part-0.parquet", compression=None, index=False
            )

        train_path = output_dir / Path("train") / Path(grain_dir)
        train_path.mkdir(parents=True, exist_ok=True)
        train_df.to_parquet(
            train_path / "part-0.parquet", compression=None, index=False
        )

        result_list.append("{}: {}".format(grain_dir, "done"))
    return result_list
