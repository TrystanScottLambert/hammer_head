"""
Module for combining ALL the shark data into a single massive file.
"""

import glob

import h5py
import pandas as pd
import numpy as np


def scrape_all_mock_data(
    directory: str, mock_file_prefix: str       
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Goes through all the mock hdf5 files in the directory and creates
    a DataFrame of all the values. Essentially one large table.
    """
    mock_hdf5_list = np.array(glob.glob(f"{directory}{mock_file_prefix}*.hdf5"))
    mock_hdf5_list_sort_idx = np.argsort([f"{name.split('.')[1]:0>2}" for name in mock_hdf5_list])
    mock_hdf5_list = mock_hdf5_list[mock_hdf5_list_sort_idx]
    galaxy_dfs = []
    group_dfs = []
    for file_name in mock_hdf5_list:
        galaxies = {}
        groups = {}
        with h5py.File(file_name) as file:
            for column in file["galaxies"]:
                galaxies[column] = file["galaxies"][column][:]

            for column in file["groups"]:
                groups[column] = file["groups"][column][:]

        galaxy_dfs.append(pd.DataFrame(galaxies))
        group_dfs.append(pd.DataFrame(groups))
    return pd.concat(galaxy_dfs, ignore_index=True), pd.concat(
        group_dfs, ignore_index=True
    )


def scrape_all_sed_data(directory: str, sed_file_prefix: str) -> pd.DataFrame:
    """
    Goes through all the sed files in the directory and crates a data frame.
    """
    sed_hdf5_list = np.sort(glob.glob(f"{directory}{sed_file_prefix}*.hdf5"))
    with h5py.File(sed_hdf5_list[0]) as file:
        filters = file["filters"][:]
    filters = [name.decode() for name in filters]

    sed_dfs = []
    for file_name in sed_hdf5_list:
        print(f'Doing {file_name}')
        df = {}
        with h5py.File(file_name) as file:
            for i, name in enumerate(filters):
                df[f"{name}_apparent_mag"] = file["SED"]["ap_dust"]["total"][i]
                df[f"{name}_absolute_mag"] = file["SED"]["ab_dust"]["total"][i]
            sed_dfs.append(pd.DataFrame(df))
    return pd.concat(sed_dfs, ignore_index=True)


def build_big_tables(
    out_directory: str, directory: str, mock_prefix: str, sed_prefix: str
) -> None:
    """
    Scrapes the group and galaxy data including the SED data and builds a data central .csv file.
    """
    print("Scraping mock data:")
    gal_df, group_df = scrape_all_mock_data(directory, mock_prefix)

    print("Scraping SED data:")
    sed_df = scrape_all_sed_data(directory, sed_prefix)
    assert(len(sed_df) == len(gal_df))

    final_gal = pd.concat([gal_df, sed_df], axis=1)

    print("writing galaxies:")
    final_gal.to_csv(f"{out_directory}galaxies.csv", index=False)

    print("writing groups:")
    group_df.to_csv(f"{out_directory}groups.csv", index=False)


if __name__ == "__main__":
    #build_big_tables("./", "shark_hdf5/", "mocksky", "Sting")
    IN_DIR = "/scratch/pawsey0119/clagos/Stingray/medi-SURFS/Sharkv2-Lagos23-HBTTrees-bestparams/waves-wide/"
    OUT_DIR = "/scratch/pawsey0119/tlambert/mock_catalogs/data_central/"
    build_big_tables(OUT_DIR, IN_DIR,  "mocksky", "Sting-SED-eagle-rr14_")
