"""
Module for scraping the meta data from the hdf5 files and bulding the final meta_data table.
"""

from enum import Enum
from dataclasses import dataclass
from datetime import date

import json

import h5py

def today() -> str:
    """
    Returns todays date as a string in the YYYY-MM-DD
    """
    return date.today().strftime("%Y-%m-%d")

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
    name: str
    file_name: str
    description: str
    table_date: str = today()
    documentation: str = ''

@dataclass
class Group:
    """
    Group data-structure which stores tables.
    """
    name: str
    pretty_name: str
    tables: list[Table]
    description: str
    contact: str = 'Trystan Lambert <trystan.lambert@uwa.edu.au>'
    documentation: str = ''
    group_date:str = today()

def write_directory_meta_data(groups: list[Group], outfile_prefix: str, version: str) -> None:
    """
    Creates the table and group meta data .txt files for the given list of groups.
    outfile_prefix = 'shark' would give you shark_table_meta.txt and shark_group_meta.txt.
    """
    group_file_name = f'{outfile_prefix}_group_meta.txt'
    table_file_name = f'{outfile_prefix}_table_meta.txt'

    with open(group_file_name, 'w', encoding='utf8') as file:
        file.write('name|pretty_name|description|documentation|contact|date|version\n')
        for group in groups:
            file.write(f"{group.name}|{group.pretty_name}|{group.description}|{group.documentation}|{group.contact}|{group.group_date}|{version}\n")
        
    with open(table_file_name, 'w', encoding='utf8') as file:
        file.write('name|description|documentation|group_name|filename|contact|date|version\n')
        for group in groups:
            for table in group.tables:
                file.write(f"{table.name}|{table.description}|{table.documentation}|{group.name}|{table.file_name}|{group.contact}|{table.table_date}|{version}\n")



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
    # Column Meta Data.
    INFILE = "shark_hdf5/mocksky.0.hdf5"
    INFILE_SED = "shark_hdf5/Sting-SED-eagle-rr14_00.hdf5"

    deep_gals_name = "WavesDeepGals"
    deep_groups_name = "WavesDeepGalGroups"
    wide_gals_name = "WavesWideGals"
    wide_groups_name = "WavesWideGalGroups"

    table_wdg = create_metadata_table_from_mock(deep_gals_name, INFILE, FileType.GALAXIES)
    table_wdgg = create_metadata_table_from_mock(
        deep_groups_name, INFILE, FileType.GROUPS
    )
    table_wwg = create_metadata_table_from_mock(
        wide_gals_name, INFILE, FileType.GALAXIES
    )
    table_wwgg = create_metadata_table_from_mock(
        wide_groups_name, INFILE, FileType.GROUPS
    )

    table_sed_wd = create_metadata_table_from_sed(deep_gals_name, INFILE_SED)
    table_sed_ww = create_metadata_table_from_sed(wide_gals_name, INFILE_SED)

    meta = [table_wdg, table_sed_wd, table_wdgg, table_wwg, table_sed_ww, table_wwgg]
    write_meta_data("waves_shark_column_meta.txt", meta)

    # Table and Group meta data.
    table_deep_galaxies = Table(deep_gals_name, f"{deep_gals_name}.csv", "This catalogue contains galaxies in the WAVES-deep mock lightcone with a pseudo-magnitude cut of <30 and up to a redshift of z=2")
    table_deep_groups = Table(deep_groups_name, f"{deep_groups_name}.csv", "This catalogue contains the groups in the WAVES-deep mock lightcone, up to a redshift of z=2")
    table_wide_galaxies = Table(wide_gals_name, f"{wide_gals_name}.csv", "This catalogue contains galaxies in the WAVES-wide mock lightcone with a pseudo-magnitude cut of <30 and up to a redshift of z=1")
    table_wide_groups = Table(wide_groups_name, f"{wide_groups_name}.csv", "This catalogue contains the groups in the WAVES-wide mock lightcone, up to a redshift of z=1")

    with open("preamble.txt", encoding='utf8') as pre_file:
        preamble = pre_file.readlines()
    wide_group = Group("waves-wide", "WAVES Wide", [table_wide_galaxies, table_wide_groups], f"Shark mock lightcones in the geometry of WAVES-wide. Please see instructions for citing (https://waves.wiki.org.au/en/Working-Groups/TWG8-Numerical-Simulations/Citing).")
    deep_group = Group("waves-deep", "WAVES Deep", [table_deep_galaxies, table_deep_groups], f"Shark mock lightcones in the geometry of WAVES-deep. Please see instructions for citing (https://waves.wiki.org.au/en/Working-Groups/TWG8-Numerical-Simulations/Citing).")

    write_directory_meta_data([deep_group, wide_group], "waves_shark", "v0.3.1")
