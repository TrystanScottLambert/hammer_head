"""
Module for scraping the meta data from the hdf5 files.
"""

from enum import Enum
from dataclasses import dataclass

import json

import h5py

@dataclass
class ColumnMetaData:
    """
    Main data-structure for storing the meta data for each column.
    """
    name: str
    description: str
    unit: str
    ucd: str
    data_type: str

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


def _scrape_unit_from_description(descripition: str) -> str:
    """
    Checks if there is a unit in the description string and returns None string if none.
    """
    if '[' in descripition:
        return descripition.split('[')[-1].split(']')[0]
    return 'None'


def read_json_file(file_name: str) -> dict:
    """
    Reading in the different data stored as json in the module.
    """
    with open(file_name, 'r', encoding='utf8') as file:
        data = json.load(file)
    return data


def create_metadata_table_from_mock(table_name: str, file_name: str, file_type: FileType) -> Table:
    """
    Returns the full list of properties from the example file.
    """
    data_types = read_json_file('data_types.json')
    mock_ucds = read_json_file(f'{file_type}_ucd.json')
    columns = []
    with h5py.File(file_name) as file:
        for key in file[file_type]:
            name = key
            description = file[file_type][key].attrs['Comment'][0].decode()
            unit = _scrape_unit_from_description(description)
            ucd = mock_ucds[key]
            data_type = data_types[str(type(file[file_type][key][0]))]
            columns.append(ColumnMetaData(name, description, unit, ucd, data_type))

    return Table(table_name, columns)

