"""
Module for scraping the meta data from the hdf5 files.
"""

from enum import Enum
from dataclasses import dataclass

import json

import h5py


BAND_RANGES = {
    "Band9_ALMA": "602 GHz - 720 GHz",
    "Band8_ALMA": "385 GHz - 500 GHz",
    "Band7_ALMA": "275 GHz - 373 GHz",
    "Band6_ALMA": "211 GHz - 275 GHz",
    "Band5_ALMA": "163 GHz - 211 GHz",
    "Band4_ALMA": "125 GHz - 163 GHz",
    "Band3_ALMA": "84 GHz - 116 GHz",
    "BandX_VLA":"24.98 mm - 37.47 mm",
    "BandC_VLA":"37.47 mm - 74.95 mm",
    "BandS_VLA":"74.95 mm - 149.9 mm",
    "BandL_VLA":"149.9 mm - 299.79 mm",
    "Band_610MHz":"454.23 mm - 535.34 mm",
    "Band_325MHz":"799.45 mm - 1090.15 mm",
    "Band_150MHz":"1498.96 mm - 2997.92 mm",
}


@dataclass
class ColumnMetaData:
    """
    Main data-structure for storing the meta data for each column.
    """

    name: str
    description: str
    table_name: str
    unit: str
    ucd: str
    data_type: str

    def to_dict(self):
        """returns a dictionary representation of the class."""
        return {
            "name": self.name,
            "description": self.description,
            "table_name": self.table_name,
            "ucd": self.ucd,
            "unit": self.unit,
            "data_type": self.data_type,
        }


@dataclass
class Table:
    """
    Table data-structure for the meta data
    """

    table_name: str
    meta_data: list[ColumnMetaData]


class FileType(Enum):
    """
    The types of hdf5 files we can have. Namely groups or galaxies
    """

    GALAXIES: str = "galaxies"
    GROUPS: str = "groups"


def separate_unit_and_description(descripition: str) -> tuple[str, str]:
    """
    Checks if there is a unit []. Then returns the description minus unit and the unit.
    Will return unit separately.

    E.g. 'ra of the galaxy [deg]' => ('ra of the galaxy', 'deg')
    """

    if "[" in descripition:
        unit = descripition.split("[")[-1].split("]")[0]
        desc = descripition.strip(f"[{unit}]").strip()
        return desc, unit
    return descripition, "None"


def read_json_file(file_name: str) -> dict:
    """
    Reading in the different data stored as json in the module.
    """
    with open(file_name, "r", encoding="utf8") as file:
        data = json.load(file)
    return data


def create_metadata_table_from_mock(
    table_name: str, file_name: str, file_type: FileType
) -> list[ColumnMetaData]:
    """
    Returns a list of column meta data with all the metadata scraped from the mock.
    Either galaxies or groups.
    """
    file_type_string = file_type.value
    data_types = read_json_file("data_types.json")
    mock_ucds = read_json_file(f"{file_type.value}_ucd.json")
    columns = []
    with h5py.File(file_name) as file:
        for key in file[file_type_string]:
            local_dir = file[file_type_string]
            name = key
            description, unit = separate_unit_and_description(
                local_dir[key].attrs["Comment"][0].decode()
            )
            ucd = mock_ucds[key]
            data_type = data_types[type(local_dir[key][0]).__name__]
            columns.append(
                ColumnMetaData(name, description, table_name, unit, ucd, data_type)
            )
    return columns


def create_metadata_table_from_sed(
    table_name: str, file_name: str
) -> list[ColumnMetaData]:
    """
    Returns a table of the meta data scraped from the SED file.
    """
    with h5py.File(file_name) as file:
        filter_names = list(file["filters"][:])
    filter_names = [name.decode() for name in filter_names]
    meta_data = []
    for name in filter_names:
        for mag_type, ucd_mag_type in zip(
            ["apparent", "absolute"], ["phot.mag", "phys.magAbs"]
        ):
            local_name = f"{name}_{mag_type}_mag"
            if ("ALMA" in name) | ("VLA" in name) | ("MHz" in name):
                desc = f"{mag_type.capitalize()} magnitude in the top-hat filter over the range {BAND_RANGES[name]}"

            else:
                desc = f"{mag_type.capitalize()} magnitude in filter {name} (includes dust)"
            meta_data.append(
                ColumnMetaData(
                    local_name, desc, table_name, "mag", ucd_mag_type, "float32"
                )
            )

    return meta_data


def write_meta_data(
    outfile_name: str, list_of_tables: list[list[ColumnMetaData]]
) -> None:
    """
    Reads in all the metadata tables and creates a single yml file with all the meta data.
    """
    with open(outfile_name, "w", encoding="utf8") as file:
        file.write("name|description|table_name|ucd|unit|data_type\n")
        for table in list_of_tables:
            for meta_data in table:
                file.write(
                    f"{meta_data.name}|{meta_data.description}|{meta_data.table_name}|{meta_data.ucd}|{meta_data.unit}|{meta_data.data_type}\n"
                )

if __name__ == "__main__":
    INFILE = "shark_hdf5/mocksky.0.hdf5"
    INFILE_SED = "shark_hdf5/Sting-SED-eagle-rr14_00.hdf5"
    table_wdg = create_metadata_table_from_mock(
        "WavesDeepGals", INFILE, FileType.GALAXIES
    )
    table_wdgg = create_metadata_table_from_mock(
        "WavesDeepGroups", INFILE, FileType.GROUPS
    )
    table_wwg = create_metadata_table_from_mock(
        "WavesWideGals", INFILE, FileType.GALAXIES
    )
    table_wwgg = create_metadata_table_from_mock(
        "WavesWideGroups", INFILE, FileType.GROUPS
    )

    table_sed_wd = create_metadata_table_from_sed("WavesDeepGals", INFILE_SED)
    table_sed_ww = create_metadata_table_from_sed("WavesWideGals", INFILE_SED)

    meta = [table_wdg, table_sed_wd, table_wdgg, table_wwg, table_sed_ww, table_wwgg]
    write_meta_data("test.dat", meta)
