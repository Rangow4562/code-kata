import csv
import pathlib
import codecs
import dataclasses
import json
import random as rnd
import string
import logging
from itertools import accumulate, chain
from typing import Any, Callable, Iterator, ClassVar, List

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class FWFColumnSpec:
    """Fixed Width File Column specification"""
    name: str
    offset: int
    length: int
    dtype: str = "str"

@dataclasses.dataclass
class FWFSpec:
    """Fixed Width File specification"""
    columns: List[FWFColumnSpec]
    header: bool
    encoding: str

@dataclasses.dataclass
class CSVSpec:
    """CSV file specification"""
    column_names: List[str]
    header: bool
    encoding: str
    delimiter: str = ","
    quotechar: str = '"'

def _rnd_str(length=1) -> str:
    return "".join(rnd.choice(string.ascii_lowercase) for _ in range(length))

def _rnd_fwf_str(column_spec: FWFColumnSpec) -> str:
    return _rnd_str(column_spec.length)

_RND_VALUES_GENERATOR_BY_TYPE = {
    "str": _rnd_fwf_str,
}

def _unexpected_data_type(column_spec: FWFColumnSpec):
    raise ValueError(f"Unexpected datatype {column_spec.dtype} for column {column_spec.name}")

def rnd_fwf_value(column_spec: FWFColumnSpec) -> str:
    rnd_generator = _RND_VALUES_GENERATOR_BY_TYPE.get(column_spec.dtype, _unexpected_data_type)
    return rnd_generator(column_spec)

def validate_encoding(encoding: str):
    try:
        codecs.lookup(encoding)
    except LookupError:
        raise ValueError(f"Encoding {encoding} not found")

def load_csv_spec_file(spec_path: pathlib.Path) -> CSVSpec:
    try:
        with open(spec_path, "r") as f:
            return load_csv_spec_json(f.read())
    except Exception as e:
        logger.error(f"Error loading CSV spec file: {e}")
        raise

def load_csv_spec_json(spec_json: str) -> CSVSpec:
    try:
        data = json.loads(spec_json)
        column_names = data.get("ColumnNames")
        header = data.get("IncludeHeader")
        encoding = data.get("DelimitedEncoding")

        validate_encoding(encoding)

        return CSVSpec(
            column_names=column_names,
            header=header,
            encoding=encoding
        )
    except Exception as e:
        logger.error(f"Error loading CSV spec JSON: {e}")
        raise

def load_fwf_spec_file(spec_path: pathlib.Path) -> FWFSpec:
    try:
        with open(spec_path, "r") as f:
            return load_fwf_spec_json(f.read())
    except Exception as e:
        logger.error(f"Error loading FWF spec file: {e}")
        raise

def load_fwf_spec_json(spec_json: str) -> FWFSpec:
    try:
        data = json.loads(spec_json)
        column_names = data.get("ColumnNames")
        header = data.get("IncludeHeader")
        encoding = data.get("FixedWidthEncoding")
        column_lengths = data.get("Offsets")

        validate_encoding(encoding)

        if len(column_names) != len(column_lengths):
            raise ValueError("Offsets length must be the same as ColumnNames length")

        column_offsets = [0] + list(accumulate(column_lengths))[:-1]
        spec_values = zip(column_names, column_offsets, column_lengths)
        columns = [FWFColumnSpec(*col) for col in spec_values]

        return FWFSpec(header=header, encoding=encoding, columns=columns)
    except Exception as e:
        logger.error(f"Error loading FWF spec JSON: {e}")
        raise

def write_csv_file(spec: CSVSpec, lines: Iterator[Iterator[Any]], csv_output_file: pathlib.Path):
    try:
        if csv_output_file.parent:
            csv_output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(csv_output_file, "w", newline="", encoding=spec.encoding) as f:
            writer = csv.writer(f, delimiter=spec.delimiter, quotechar=spec.quotechar)
            if spec.header:
                writer.writerow(spec.column_names)
            writer.writerows(lines)
    except Exception as e:
        logger.error(f"Error writing CSV file: {e}")
        raise

def _create_fwf_header(spec: FWFSpec) -> str:
    header = ""
    for col in spec.columns:
        header += col.name.ljust(col.length, " ")
    return header

def _generate_fwf_lines(
    spec: FWFSpec, number_of_lines: int, rnd_value_generator: Callable[[FWFColumnSpec], str]
) -> Iterator[str]:
    for _ in range(number_of_lines):
        line = ""
        for col in spec.columns:
            line += rnd_value_generator(col)
        yield line

def generate_fwf_lines(
    spec: FWFSpec,
    number_of_lines: int,
    rnd_value_generator: Callable[[FWFColumnSpec], str] = rnd_fwf_value,
) -> Iterator[str]:
    if number_of_lines <= 0:
        raise ValueError("number_of_lines should be > 0")
    rows_generator = _generate_fwf_lines(spec, number_of_lines, rnd_value_generator)
    if spec.header:
        return chain([_create_fwf_header(spec)], rows_generator)
    else:
        return rows_generator

def generate_fwf_file(
    spec: FWFSpec,
    number_of_lines: int,
    output_file: pathlib.Path,
    rnd_value_generator: Callable[[FWFColumnSpec], str] = rnd_fwf_value,
) -> None:
    try:
        if output_file.parent:
            output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", 1024, encoding=spec.encoding, newline="") as f:
            lines = generate_fwf_lines(spec, number_of_lines, rnd_value_generator)
            f.writelines((line + "\n" for line in lines))
    except Exception as e:
        logger.error(f"Error generating FWF file: {e}")
        raise

def parse_fwf_file(
    spec: FWFSpec,
    input_file: pathlib.Path,
) -> Iterator[Iterator[Any]]:
    try:
        slices = [slice(col.offset, col.offset + col.length, None) for col in spec.columns]
        with open(input_file, "r", 1024, encoding=spec.encoding) as f:
            if spec.header:
                next(f)
            for line in f:
                yield (line[s].strip() for s in slices)
    except Exception as e:
        logger.error(f"Error parsing FWF file: {e}")
        raise
